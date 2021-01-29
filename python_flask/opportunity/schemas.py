"""
market_crm.opportunities.schemas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Schemas for serializing opportunities
"""
import json
from marshmallow import (Schema, fields, post_load, pre_load, validate,)

from market_crm.schemas import (
    ObjectIdField, NaiveDateTime, DictOfField,
    UniqueListField, DateFilterSchema,
    StringifiedSchema, order_validator
)
from market_crm.opportunities.model import Opportunity


class GuestSheetSchema(Schema):
    vehicle_color = fields.List(fields.Str)
    vehicle_type = fields.List(fields.Str)
    vehicle_style = fields.List(fields.Str)
    passenger_count_upper = fields.Int()
    passenger_count_lower = fields.Int()
    vehicle_features = DictOfField(fields.Str)
    vehicle_features_extra = fields.List(fields.Str)
    vehicle_preference_questionnaire = DictOfField(fields.Str)
    preferred_vehicles = fields.List(fields.Str)

    home = fields.String()
    monthly_payment = fields.Float(allow_none=True)
    employer = fields.String(allow_none=True)
    job_title = fields.String(allow_none=True)
    monthly_income = fields.Float(allow_none=True)
    on_the_job_3_months = fields.Bool()
    co_signer = fields.Bool()
    monthly_budget = fields.String()


class OpportunityMarketingSchema(Schema):
    lead_direction = fields.Str()
    lead_channel = fields.Str()
    lead_source = fields.Str()
    campaign_medium = fields.Str()
    campaign_source = fields.Str()
    campaign_name = fields.Str()


class DMSDealSchema(Schema):
    customer_province = fields.Str()
    sales_deal_type = fields.Str()
    vin = fields.Str()
    sum_payments = fields.Float()
    lienholder = fields.Str()
    date_sold = fields.Str()
    rate = fields.Float() # : None,
    cost = fields.Float(allow_none=True)
    year = fields.Int(allow_none=True)
    payment_frequency = fields.Str()
    stock_number = fields.Str()
    payment_style = fields.Str()
    odometer = fields.Int()
    payment_amount = fields.Float()
    deal_type = fields.Str()
    cash_price = fields.Float()
    payment_type = fields.Str()
    sale_type = fields.Str()
    deposit_amount = fields.Float()
    frontend_gross = fields.Float()
    total_gross = fields.Float()
    downpayment = fields.Float()
    backend_gross = fields.Float()
    total_payments = fields.Int()
    trades = fields.List(fields.Dict)
    model_name = fields.Str(allow_none=True)
    deal_number = fields.Str()
    make_name = fields.Str(allow_none=True)
    msrp = fields.Float()


class UserGrossSchema(Schema):
    """Schema for user-proved gross values used on the accounting_deal and sales_deal
    """
    updated_by = fields.Str()
    updated_by_name = fields.Str()
    updated = NaiveDateTime()
    value = fields.Float()


class UserDealCommentSchema(Schema):
    updated_by = fields.Str()
    updated_by_name = fields.Str()
    updated = NaiveDateTime()
    content = fields.Str()


class UserDealSchema(Schema):
    """The ManualDealSchema is used for both the accounting_deal and sales_deal
    as both allow for manualy data entry by a user, where the DMSDealSchema is for deal
    data synced from the DMS
    """
    frontend_gross = fields.Nested(UserGrossSchema)
    backend_gross = fields.Nested(UserGrossSchema)
    comment = fields.Nested(UserDealCommentSchema)


class OpportunityAttachmentSchema(Schema):
    _id = ObjectIdField(dump_only=True, simple=True)
    attachment_type = fields.Str(missing=Opportunity.ATTACHMENT_TYPE.DEFAULT)
    key = fields.Str(required=True)
    label = fields.Str(allow_none=True)
    created_by = fields.Str()
    created_by_name = fields.Str()
    file_hash = fields.Str(allow_none=True)
    file_size = fields.Int(allow_none=True)
    content_type = fields.Str(allow_none=True)
    date_created = NaiveDateTime(dump_only=True)
    deleted = fields.Bool(dump_only=True)
    file_tag = fields.Str(allow_none=True)


class RDRPunchSchema(Schema):
    class Meta:
        strict = True

    punch_date = NaiveDateTime(required=True)
    username = fields.Str(required=True)
    notes = fields.Str()
    plate_number = fields.Str()
    amount = fields.Str()
    program = fields.Str()
    assigned_to = fields.Str()


class ReportingPeriodSchema(Schema):
    class Meta:
        strict = True

    year = fields.Int(required=True)
    month = fields.Int(required=True)
    quarter = fields.Int(dump_only=True)


class OpportunitiesPermissionsSchema(Schema):
    can_assign_user = fields.Bool(dump_only=True)
    can_assign_salesperson = fields.Bool(dump_only=True)
    can_assign_bdc_rep = fields.Bool(dump_only=True)
    can_edit_deal_number = fields.Bool(dump_only=True)
    can_add_opportunity_attachment = fields.Bool(dump_only=True)
    can_view_opportunity_attachment = fields.Bool(dump_only=True)
    can_delete_opportunity_attachment = fields.Bool(dump_only=True)
    can_edit_finance_checklist = fields.Bool(dump_only=True)
    can_read_details = fields.Bool(dump_only=True)

class OpportunitySchema(Schema):
    _id = ObjectIdField(simple=True)
    customer_id = ObjectIdField(required=True, simple=True)
    name = fields.Str()
    customer_name = fields.Str()
    customer_keywords = fields.List(fields.Str, default=[])
    status = fields.Int()
    sub_status = fields.Str(allow_none=True)
    lost_reason = fields.Str()
    creator = fields.Str()
    dealer_id = fields.Int(required=True)
    dealer_name = fields.Str()
    stock_type = fields.Str()
    primary_pitch_id = fields.Str(allow_none=True)
    sales_managers = UniqueListField(fields.Str, default=[])
    sales_reps = UniqueListField(fields.Str, default=[])
    customer_reps = UniqueListField(fields.Str, default=[])
    bdc_reps = UniqueListField(fields.Str, default=[])
    finance_managers = UniqueListField(fields.Str, default=[])
    pitches = UniqueListField(fields.Str, default=[])
    leads = UniqueListField(fields.Str, default=[])
    crm_lead_ids = UniqueListField(ObjectIdField, default=[])
    appraisals = UniqueListField(fields.Str, default=[])
    credit_applications = UniqueListField(fields.Str, default=[])
    preferences = fields.Nested(GuestSheetSchema)
    marketing = fields.Nested(OpportunityMarketingSchema)
    updated = NaiveDateTime()
    last_status_change = DictOfField(NaiveDateTime)
    created = NaiveDateTime()
    reporting_period = fields.Nested(ReportingPeriodSchema)
    dms_deal = fields.Nested(DMSDealSchema, allow_none=False)
    accounting_deal = fields.Nested(UserDealSchema)
    sales_deal = fields.Nested(UserDealSchema)
    carryover_date = NaiveDateTime(allow_none=True)
    attachments = fields.Nested(OpportunityAttachmentSchema, many=True)
    gocard_referral = fields.Dict(allow_none=True)
    rdr_punch = fields.Nested(RDRPunchSchema)
    finance_checklist = fields.Dict(default={})
    accounting_checklist = fields.Dict(default={})
    extra_checklist = fields.Dict(default={})
    organization_id = fields.Str(required=True)
    alert_types = fields.List(fields.Str, default=[])
    permissions = fields.Nested(OpportunitiesPermissionsSchema, dump_only=True)
    is_fresh_up = fields.Bool(dump_only=True)
    is_sales_rep_slot_available = fields.Bool(dump_only=True)
    assignees = fields.List(fields.Str, default=[], dump_only=True)
    cursor_key = fields.Str(dump_only=True)
    test_drive_number = fields.Int()

class OpportunityUpdateSchema(OpportunitySchema):
    class Meta:
        strict = True
        dump_only = (
            '_id', 'customer_id', 'organization_id',
            'last_status_change', 'updated', 'created'
        )

    dealer_id = fields.Int()

    # Used to set `last_status_change` on update
    status_date_change = NaiveDateTime(load_only=True)

    # Used to change `deal_number` on update
    deal_number = fields.Str(load_only=True)


class EditDealNumberSchema(Schema):
    class Meta:
        strict = True

    deal_number = fields.Str(load_only=True, required=True)


class OpportunityOrderingSchema(StringifiedSchema):
    class Meta:
        strict = True

    created = fields.Int(validate=order_validator())
    dealer_name = fields.Int(validate=order_validator())
    customer_name = fields.Int(validate=order_validator())


class ReportingPeriodFilterSchema(Schema):
    class Meta:
        strict = True

    year = fields.Int(required=True)
    month = fields.Int()
    quarter = fields.Int()


class OpportunitiesFilterSchema(StringifiedSchema):
    class Meta:
        strict = True

    @pre_load
    def wrap_loose_reporting_period_params(self, data):
        if not 'reporting_period' in data and set(('month', 'year', 'quarter',)) & set(data.keys()):
            updated_data = {k: v for k, v in data.items()}
            updated_data ['reporting_period'] = {}
            if 'month' in data:
                updated_data['reporting_period']['month'] = updated_data.pop('month')
            if 'year' in data:
                updated_data['reporting_period']['year'] = updated_data.pop('year')
            if 'quarter' in data:
                updated_data['reporting_period']['quarter'] = updated_data.pop('quarter')
            return updated_data

    ids = fields.List(ObjectIdField)
    organization_id = fields.Str(required=True)
    dealer_ids = fields.List(fields.Int, required=True)
    statuses = fields.List(fields.Int)
    assignees = fields.List(fields.Str)
    bdc_assignees = fields.List(fields.Str)
    status_date = fields.Nested(DateFilterSchema)
    created = fields.Nested(DateFilterSchema)
    updated = fields.Nested(DateFilterSchema)
    lead_channel = fields.Str()
    lead_direction = fields.Str()
    lead_source = fields.Str()
    sub_status = fields.Str()
    assigned_to_bdc = fields.Boolean()
    stock_type = fields.Str(allow_none=True)  # None == no stock type
    reporting_period = fields.Nested(ReportingPeriodFilterSchema)
    keywords = fields.Str()
    pitches = fields.List(fields.Str)
    leads = fields.List(fields.Str)
    crm_lead_ids = fields.List(ObjectIdField)
    credit_applications = fields.List(fields.Str)
    customer_ids = fields.List(ObjectIdField)
    created_by = fields.List(fields.Str)


class OpportunityCursorSchema(OpportunitySchema):
    customer_name = fields.Str(allow_none=True)


class OpportunitiesParamsSchema(StringifiedSchema):
    class Meta:
        strict = True

    # V1 and V2 api are using the same schema for params.  This pre_load function
    # ensures that the dao gets the sort_by param in a consistent way.
    @pre_load
    def sort_by_to_array(self, data):
        if 'sort_by' in data and type(data['sort_by']) != list:
            updated_data = {k: v for k, v in data.items()}
            updated_data['sort_by'] = [data['sort_by']]
            return updated_data

    page = fields.Int(missing=1, validate=validate.Range(min=1, error="page must be greater than {min}"))
    page_size = fields.Int(missing=0, validate=validate.Range(min=0, error="page_size must be greater than {min}"))
    sort_by = fields.Nested(OpportunityOrderingSchema, missing=[dict(created=-1)], many=True)
    filters = fields.Nested(OpportunitiesFilterSchema, required=True)


class OpportunitiesByCursorParamsSchema(StringifiedSchema):
    class Meta:
        strict = True

    @post_load
    def cursor_key_string_to_partial_opportunity_dict(self, data):
        if 'cursor_key' in data:
            updated_data = {k: v for k, v in data.items()}
            updated_data['cursor_key'] = OpportunityCursorSchema().loads(updated_data['cursor_key'], partial=True).data
            return updated_data

    cursor_key = fields.Str() # used in cursor based pagination
    get_more = fields.Str() # before or after - uses page_size for limiting value # TODO: validation
    size = fields.Int(missing=100)
    sort_by = fields.Nested(OpportunityOrderingSchema, missing=[dict(created=-1)], many=True)
    filters = fields.Nested(OpportunitiesFilterSchema, required=True)
