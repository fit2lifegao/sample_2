import boto
import json

from bson.objectid import ObjectId
from werkzeug.local import LocalProxy
from flask import abort, Blueprint, request, current_app, jsonify
from marshmallow import ValidationError

from market_crm.application import sentry
from market_crm.config import DAP_EIP_S3_ARCHIVE_BUCKET
from market_crm.utils import validator
from market_crm.database import db, PaginatedResults, CursorPaginatedResults
from market_crm.utils.decorator import ResponseWrapper
from market_crm.services.auth import get_current_user, User

from .schemas import (
    OpportunitySchema, OpportunityUpdateSchema, RDRPunchSchema,
    EditDealNumberSchema, OpportunityAttachmentSchema,
    OpportunityMarketingSchema,
    GuestSheetSchema, UserDealSchema,
    OpportunitiesParamsSchema, OpportunitiesFilterSchema,
    OpportunitiesByCursorParamsSchema,
)
from .model import Opportunity as OpportunityModel, OpportunityStockTypeOptions

from .deal_converter import DealConverter
from .authorization import can

def ensure(permission_check):
    if not permission_check:
        abort(403)

def permissions_for(opportunity):
    return dict(
        can_assign_user=can(current_user).assign_user(opportunity),
        can_assign_salesperson=can(current_user).assign_salesperson(opportunity),
        can_assign_bdc_rep=can(current_user).assign_bdc_rep(opportunity),
        can_edit_deal_number=can(current_user).edit_deal_number(opportunity),
        can_add_opportunity_attachment=can(current_user).add_attachment(opportunity),
        can_view_opportunity_attachment=can(current_user).view_attachment(opportunity),
        can_delete_opportunity_attachment=can(current_user).delete_attachment(opportunity),
        can_edit_finance_checklist=can(current_user).edit_finance_checklist(opportunity),
        can_read_details=can(current_user).read_details(opportunity),
        can_link_lead=can(current_user).link_lead(opportunity)
        )


opportunity_schema = OpportunitySchema()
rdr_schema = RDRPunchSchema()

mod = Blueprint("opportunityv2", __name__)
current_user = LocalProxy(get_current_user)

ROLE_ASSIGNMENT_FIELDS = {
    User.ROLE_SALES_REP: 'sales_reps',
    User.ROLE_INTERNET_SALES_REP: 'sales_reps',
    User.ROLE_CSR: 'customer_reps',
    User.ROLE_SALES_MANAGER: 'sales_managers',
    User.ROLE_BDC_REP: 'bdc_reps',
    User.ROLE_BDC_MANAGER: 'bdc_reps',
    User.ROLE_FINANCE_MANAGER: 'finance_managers',
}
@mod.errorhandler(404)
def not_found_404(message=None):
    message = message or 'Resource not found'
    return jsonify(message=message), 404


@mod.errorhandler(Exception)
def handle_exceptions(error):
    if current_app.config.get('TESTING'):
        raise error
    if 'sentry' in current_app.extensions:
        current_app.extensions['sentry'].captureException()

    if isinstance(error, ValidationError):
        message = 'Invalid data. Please fix the following errors. {}'.format(error.messages)

        return jsonify(
            message=message,
            errors=error.messages
        ), 400

    else:
        return jsonify(message=error.message), 500


def get_json_or_400():
    data = request.get_json()
    if not data:
        return jsonify(message='No input data provided.'), 400
    return data


@mod.route('/opportunities', methods=['POST'])
def create_opportunity():
    data = get_json_or_400()
    data['organization_id'] = current_user['organization']['id']
    data['creator'] = current_user['username']

    opportunity_data = OpportunitySchema(strict=True).load(data).data

    ensure(can(current_user).create(opportunity_data))

    opportunity = db.opportunity_dao.add_opportunity(**opportunity_data)
    opportunity['permissions'] = permissions_for(opportunity)

    data = opportunity_schema.dump(opportunity).data
    return jsonify({'opportunity': data}), 201


def _update_opportunity_stock_type(opportunity, lead):
    interested_vehicle_type = lead['form_data'].get('interested_vehicle_vehicle_type')
    if interested_vehicle_type:
        opportunity['stock_type'] = interested_vehicle_type.lower()
    else:
        STOCK_TYPE_LOOKUP = {
            'new_sales': OpportunityStockTypeOptions.NEW,
            'used_sales': OpportunityStockTypeOptions.USED,
            'valuation': OpportunityStockTypeOptions.USED,
            'cpo_sales': OpportunityStockTypeOptions.USED
        }
        stock_type = STOCK_TYPE_LOOKUP.get(lead['form_data'].get('lead_type',OpportunityStockTypeOptions.UNKNOWN))
        if stock_type:
            opportunity['stock_type'] = stock_type


@mod.route('/opportunities/<opportunity_id>/lead/<lead_id>', methods=['POST'])
def link_opportunity_to_lead(opportunity_id, lead_id):
    """
    lead_id is the CRM lead id (lead['_id'])
    """

    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    lead = db.lead_dao.get_lead(lead_id)

    if not lead or not opportunity:
        return not_found_404()

    _update_opportunity_stock_type(opportunity, lead)
    stock_type = opportunity.get('stock_type')
    crm_lead_ids = opportunity.get('crm_lead_ids', [])
    if not ObjectId(lead_id) in crm_lead_ids:
        crm_lead_ids.append(lead_id)

    ensure(can(current_user).link_lead(opportunity))
    update_data = {'crm_lead_ids': crm_lead_ids}
    if stock_type:
        update_data['stock_type'] = stock_type
    update_data = opportunity_schema.load(update_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **update_data)

    data = opportunity_schema.dump(opportunity).data
    return jsonify({'opportunity': data})


def populate_guestsheet_preferences_from_lead(lead):
    preferences = {}

    if lead['form_data'].get('lead_source') == 'Car Loans 411' and lead['form_data'].get('message'):
        for message_item in lead['form_data']['message'].split(','):
            try:
                key, value = message_item.strip().split(':')
            except ValueError:
                continue # malformatted field

            key = key.strip()
            value = value.strip()

            if key.endswith('Occupation'):
                if value:
                    preferences['job_title'] = value
            elif key.endswith('Employer'):
                if value:
                    preferences['employer'] = value
            elif key.endswith('Monthly Income'):
                value = value.replace('$', '')
                try:
                    value = int(float(value))
                    preferences['monthly_income'] = value
                except ValueError:
                    pass # Bad data
            elif key.endswith('Monthly Payment'):
                value = value.replace('$', '')
                try:
                    value = int(float(value))
                    preferences['monthly_payment'] = value
                except ValueError:
                    pass # Bad data
            elif key.endswith('Rent or Own'):
                if value.lower() in ['rent', 'own', 'other']:
                    preferences['home'] = value.lower()
            elif key.endswith('Monthly Budget'):
                if value:
                    preferences['monthly_budget'] = value.lower()

    return preferences


@mod.route('/opportunities/lead/<lead_id>', methods=['POST'])
def create_opportunity_for_lead(lead_id):
    """
    lead_id: lead_id is the CRM lead id (lead['_id'])

    - if the user is a sales rep or internet_sales_rep, and the customer doesn't have anyone
        from dealer_id assigned to them, assign the current_user (with dealer_id)

    - desking sends a new notification IF user is BDC and dealer_id is not in HAPPY_TO_HELP_DEALER_IDS (4175, 4125)
        - will restrict this for now so BDC can't create opportunities.  :(
        - need to figure out how notifications will work to do this.
    """
    lead = db.lead_dao.get_lead(lead_id)
    customer = db.customer_dao.get_customer(lead['customer_id'])

    if not lead or not customer:
        return not_found_404()

    data = request.get_json() or {}
    data['organization_id'] = current_user['organization']['id']
    data['creator'] = current_user['username']
    data['marketing'] = OpportunityMarketingSchema().load(lead.get('form_data', {})).data
    data['customer_id'] = customer['_id']
    data['name'] = customer['fullname']

    preferences = populate_guestsheet_preferences_from_lead(lead)
    if preferences:
        data['preferences'] = preferences

    if 'dealer_id' not in data:
        data['dealer_id'] = lead.get('dealer_id')

    data['crm_lead_ids'] = [lead_id]
    _update_opportunity_stock_type(data, lead)

    assign_to = ROLE_ASSIGNMENT_FIELDS.get(current_user['role'])
    if assign_to and not data.get(assign_to):
        data[assign_to] = [current_user['username']]

    params = OpportunitySchema(strict=True).load(data).data

    ensure(can(current_user).create(OpportunityModel(params)))
    opportunity = db.opportunity_dao.add_opportunity(**params)
    opportunity['permissions'] = permissions_for(opportunity)

    if (opportunity and current_user['role'] in User.ROLES_SALES_REPS
     and data['dealer_id'] not in customer.assigned_salespeople_dealer_ids
     and data['dealer_id'] in current_user['allowed_dealer_ids']):
        customer = db.customer_dao.assign_salesperson(customer['_id'],
         data['dealer_id'], current_user['username'])

    data = opportunity_schema.dump(opportunity).data
    return jsonify({'opportunity': data}), 201


@mod.route('/opportunities', methods=['GET'])
def get_opportunities():
    args = request.args.to_dict()
    if 'filters' in args:
        args['filters'] = json.loads(args['filters'])
        args['filters'].update({'organization_id': current_user['organization']['id']})
    if 'sort_by' in args:
        args['sort_by'] = json.loads(args['sort_by'])
    params = OpportunitiesParamsSchema().load(args).data

    filters = params['filters']
    page = params.get('page')
    page_size = params.get('page_size')
    sort_by = params['sort_by']

    ensure(can(current_user).query(filters))
    opportunities_cursor = db.opportunity_dao._get_opportunities(
        filters=filters,
        sort_by=sort_by
    )
    paginated = PaginatedResults(
        opportunities_cursor,
        page=page,
        page_size=page_size
    )
    opportunity_results = paginated.dump(
        results_schema=OpportunitySchema(),
        model=OpportunityModel
    )

    for opportunity in opportunity_results['results']:
        # opportunity_results contains schema dump, so all properties
        # become dict keys
        # permissions checks rely on model properties, so
        # must use model here
        ensure(can(current_user).read(OpportunityModel(opportunity)))
        opportunity['permissions'] = permissions_for(
            OpportunityModel(opportunity)
        )
    return jsonify(opportunity_results)


@mod.route('/opportunities-cursor', methods=['GET'])
def get_opportunities_with_cursor_pagination():
    args = request.args.to_dict()
    if 'filters' in args:
        args['filters'] = json.loads(args['filters'])
        args['filters'].update({'organization_id': current_user['organization']['id']})
    if 'sort_by' in args:
        args['sort_by'] = json.loads(args['sort_by'])
    params = OpportunitiesByCursorParamsSchema().load(args).data

    cursor_key = params.get('cursor_key')
    filters = params['filters']
    get_more = params.get('get_more')
    size = params.get('size')
    sort_by = params['sort_by']

    ensure(can(current_user).query(filters))
    opportunities_cursor = db.opportunity_dao._get_opportunities(filters=filters, sort_by=sort_by)

    paginated = CursorPaginatedResults(
        opportunities_cursor,
        sort_by,
        size=size,
        get_more=get_more,
        cursor_key=cursor_key)

    filter_query = paginated.filter_query
    if filter_query:
        paginated.filtered_cursor = db.opportunity_dao._get_opportunities(
          filter_query=filter_query, filters=filters, sort_by=sort_by)

    opportunity_results = paginated.dump(results_schema=OpportunitySchema(), model=OpportunityModel)

    for opportunity in opportunity_results['results']:
        opportunity['permissions'] = permissions_for(OpportunityModel(opportunity))
        ensure(can(current_user).read(OpportunityModel(opportunity)))
    return jsonify(opportunity_results)

@mod.route('/opportunities-bulk', methods=['POST'])
def get_opportunities_bulk():
    args = request.get_json()
    args['filters'].update({'organization_id': current_user['organization']['id']})
    schema = OpportunitiesParamsSchema(only=('filters',))
    params = schema.load(args).data

    ensure(can(current_user).query(params['filters']))
    opportunities = db.opportunity_dao.get_opportunities(**params)

    for opportunity in opportunities:
        ensure(can(current_user).read(opportunity))
        opportunity['permissions'] = permissions_for(OpportunityModel(opportunity))

    data = opportunity_schema.dump(opportunities, many=True).data

    return jsonify({'opportunities': data})


@mod.route('/opportunities/<objectid:opportunity_id>', methods=['GET'])
def get_opportunity(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    ensure(can(current_user).read(opportunity))

    if opportunity:
        opportunity['permissions'] = permissions_for(OpportunityModel(opportunity))
        data = opportunity_schema.dump(opportunity).data
        return jsonify({'opportunity': data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/'
           'deal_data/<field_name>', methods=['POST'])
def update_opportunity_deal_data(opportunity_id, field_name):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    ensure(can(current_user).update(opportunity))

    data = request.get_json()
    data = UserDealSchema(strict=True).load(data).data
    ensure(can(current_user).create(data))
    opportunity = db.opportunity_dao.update_opportunity_deal_data(opportunity_id,
                                                               data,
                                                               field_name)
    if opportunity:
        data = opportunity_schema.dump(opportunity).data
        return jsonify({'opportunity': data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>', methods=['PATCH'])
def update_opportunity(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)

    if not opportunity:
        return not_found_404('Opportunity not found.')

    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(partial=True)
    schema.context['opportunity'] = opportunity

    data = schema.load(raw_data).data

    ensure(can(current_user).update(opportunity))
    # TODO: maybe put the restrictions for dealer_id and deal_number in the 'CREATE' permission checker?
    ensure(can(current_user).create(data))
    if 'dealer_id' in data:
        ensure(can(current_user).update_dealer_id(opportunity))
        # add attempted dealer_id change for the checks below
        opportunity.update({'dealer_id': data['dealer_id']})

    if set(ROLE_ASSIGNMENT_FIELDS.values()) & set(data.keys()):
        ensure(can(current_user).assign_user(opportunity))

    if 'sales_reps' in data:
        ensure(can(current_user).assign_salesperson(opportunity))

    if 'bdc_reps' in data:
        ensure(can(current_user).assign_bdc_rep(opportunity))

    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)

    data = opportunity_schema.dump(opportunity).data
    return jsonify({'opportunity': data})


@mod.route('/opportunities/<objectid:opportunity_id>/sales-reps', methods=['GET', 'PUT'])
def sales_reps(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404('Opportunity not found.')

    sales_reps = opportunity['sales_reps']
    if request.method == 'GET':
        return jsonify({'sales_reps': sales_reps})
    ensure(can(current_user).assign_salesperson(opportunity))
    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(only=['sales_reps'])
    data = schema.load(raw_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)
    data = opportunity_schema.dump(opportunity).data

    return jsonify({'sales_reps': data['sales_reps']})


@mod.route('/opportunities/<objectid:opportunity_id>/sales-managers', methods=['GET', 'PUT'])
def sales_managers(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404('Opportunity not found.')

    sales_managers = opportunity['sales_managers']
    if request.method == 'GET':
        return jsonify({'sales_managers': sales_managers})

    ensure(can(current_user).assign_user(opportunity))
    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(only=['sales_managers'])
    data = schema.load(raw_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)
    data = opportunity_schema.dump(opportunity).data

    return jsonify({'sales_managers': data['sales_managers']})


@mod.route('/opportunities/<objectid:opportunity_id>/bdc-reps', methods=['GET', 'PUT'])
def bdc_reps(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404('Opportunity not found.')

    bdc_reps = opportunity['bdc_reps']
    if request.method == 'GET':
        return jsonify({'bdc_reps': bdc_reps})

    ensure(can(current_user).assign_bdc_rep(opportunity))
    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(only=['bdc_reps'])
    data = schema.load(raw_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)
    data = opportunity_schema.dump(opportunity).data

    return jsonify({'bdc_reps': data['bdc_reps']})


@mod.route('/opportunities/<objectid:opportunity_id>/finance-managers', methods=['GET', 'PUT'])
def finance_managers(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404('Opportunity not found.')

    finance_managers = opportunity['finance_managers']
    if request.method == 'GET':
        return jsonify({'finance_managers': finance_managers})

    ensure(can(current_user).assign_user(opportunity))
    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(only=['finance_managers'])
    data = schema.load(raw_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)
    data = opportunity_schema.dump(opportunity).data

    return jsonify({'finance_managers': data['finance_managers']})


@mod.route('/opportunities/<objectid:opportunity_id>/customer-reps', methods=['GET', 'PUT'])
def customer_reps(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404('Opportunity not found.')

    customer_reps = opportunity['customer_reps']
    if request.method == 'GET':
        return jsonify({'customer_reps': customer_reps})

    ensure(can(current_user).assign_user(opportunity))
    raw_data = get_json_or_400()
    schema = OpportunityUpdateSchema(only=['customer_reps'])
    data = schema.load(raw_data).data
    opportunity = db.opportunity_dao.update_opportunity(opportunity_id, **data)
    data = opportunity_schema.dump(opportunity).data

    return jsonify({'customer_reps': data['customer_reps']})


@mod.route('/opportunities/<objectid:opportunity_id>/preferences', methods=['GET', 'PATCH'])
def preferences(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()

    ensure(can(current_user).read(opportunity))

    if request.method == 'PATCH':
        data = get_json_or_400()
        data = GuestSheetSchema(strict=True).load(data).data
        ensure(can(current_user).update(opportunity))
        db.opportunity_dao.update_preferences(opportunity_id, **data)

    preferences = db.opportunity_dao.get_preferences(opportunity_id)

    if preferences:
        preferences_data = GuestSheetSchema().dump(preferences).data
        return jsonify({'preferences': preferences_data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/marketing', methods=['GET', 'PATCH'])
def marketing_data(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()
    ensure(can(current_user).read(opportunity))

    if request.method == 'PATCH':
        data = request.get_json()
        data = OpportunityMarketingSchema(strict=True).load(data).data
        ensure(can(current_user).update(opportunity))
        db.opportunity_dao.update_marketing_data(opportunity_id, **data)

    market_data = db.opportunity_dao.get_marketing_data(opportunity_id)

    if market_data:
        data = OpportunityMarketingSchema().dump(market_data).data
        return jsonify({'market_data': data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/attachment', methods=['PUT'])
def add_attachment(opportunity_id):
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()
    ensure(can(current_user).add_attachment(opportunity))

    data = get_json_or_400()
    data = OpportunityAttachmentSchema(strict=True).load(data).data

    opportunity = db.opportunity_dao.add_attachment(opportunity_id, **data)

    if opportunity:
        return jsonify({
            'opportunity': opportunity_schema.dump(opportunity).data
        })
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/attachment/<attachment_id>', methods=['PATCH', 'DELETE'])
def update_attachment(opportunity_id, attachment_id):

    if not validator.check_object_id(attachment_id):
        return ResponseWrapper.error(status=400, message='Invalid Attachment Id')
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()
    ensure(can(current_user).add_attachment(opportunity))


    if request.method == 'PATCH':
        data = get_json_or_400()
        schema = OpportunityAttachmentSchema(only=['label', 'file_tag'], strict=True)
        update = schema.load(data).data

        opportunity = db.opportunity_dao.modify_attachment(opportunity_id, attachment_id, **update)
    elif request.method == 'DELETE':
        ensure(can(current_user).delete_attachment(opportunity))
        opportunity = db.opportunity_dao.remove_attachment(opportunity_id, attachment_id)

    if opportunity:
        return jsonify({
            'opportunity': opportunity_schema.dump(opportunity).data
        })
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/edit_deal_number', methods=['POST'])
def edit_deal_number(opportunity_id):
    """
    MPDESK-1376 Temporary functionality to edit deal number
    :param opportunity_id: string, opportunity ID
    :return:
    """
    data = get_json_or_400()
    data = EditDealNumberSchema().load(data).data

    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()
    ensure(can(current_user).edit_deal_number(opportunity))

    opportunity = db.opportunity_dao.edit_deal_number(opportunity_id, data['deal_number'])

    if opportunity:
        response_data = opportunity_schema.dump(opportunity).data
        return jsonify({'opportunity': response_data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/rdr_punch',
           methods=['POST', 'DELETE'])
def rdr_punch(opportunity_id):
    """Set or clear opportunity RDR punch"""
    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)
    if not opportunity:
        return not_found_404()
    ensure(can(current_user).view_deal_log(opportunity))

    rdr_punch = {}

    if request.method == 'POST':
        data = get_json_or_400()
        rdr_punch = rdr_schema.load(data).data

    opportunity = db.opportunity_dao.update_opportunity(
        id=opportunity_id, rdr_punch=rdr_punch
    )

    if opportunity:
        response_data = opportunity_schema.dump(opportunity).data
        return jsonify({'opportunity': response_data})
    else:
        return not_found_404()


@mod.route('/opportunities/<objectid:opportunity_id>/gross-profit')
def gross_profit_for_deal(opportunity_id):
    """Fetch Gross Profit of opportunity deal from S3"""
    debug = request.args.get('debug', False)

    opportunity = db.opportunity_dao.get_opportunity(opportunity_id)

    if not opportunity:
        return not_found_404('Opportunity not found.')

    ensure(can(current_user).view_deal_log(opportunity))
    dealer_id = opportunity['dealer_id']
    deal_id = opportunity['dms_deal'].get('deal_number')

    try:
        conn = boto.connect_s3()
        bucket = conn.get_bucket(DAP_EIP_S3_ARCHIVE_BUCKET, validate=False)
        key = '/{0}/VehicleSales/FI-WIP*{1}'.format(dealer_id, deal_id)
        s3_item = bucket.get_key(key)
    except Exception:
        sentry.captureException()
        s3_item = None

    response_data = {}

    if s3_item:
        deal = DealConverter(xml_string=s3_item.get_contents_as_string(), debug=debug)
        response_data = deal.to_representation()

    return jsonify({'gross_profit': response_data})
