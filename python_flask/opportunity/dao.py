import shlex
import re
import copy
from bson.objectid import ObjectId
from datetime import datetime, timedelta

from market_crm import signals
from .model import Opportunity as OpportunityModel, OpportunityStockTypeOptions
from .schemas import OpportunitySchema
from market_crm.database.base_dao_pymongo import MongoDAO
from market_crm.utils.helper import (reporting_period,
                                     get_date_filter,
                                     dealer_name,
                                     dictdelta)

OPPORTUNITY = "opportunity"


class MongoOpportunity(MongoDAO):
    """
    MongoDB adapter for Opportunity collection.
    """
    OPPORTUNITY_DEFAULTS = dict(
        name='',
        customer_name='',
        customer_id=None,
        customer_keywords=[],
        status=OpportunityModel.STATUS.FRESH,
        last_status_change={},
        sub_status='',
        lost_reason='',
        creator='',
        stock_type='',
        primary_pitch_id='',
        sales_managers=[],
        sales_reps=[],
        customer_reps=[],
        bdc_reps=[],
        finance_managers=[],
        pitches=[],
        leads=[],
        crm_lead_ids=[],
        appraisals=[],
        credit_applications=[],
        preferences={
            'vehicle_color': [],
            'vehicle_type': [],
            'vehicle_style': [],
            'passenger_count_upper': 0,
            'passenger_count_lower': 0,
            'vehicle_features': {},
            'vehicle_features_extra': [],
            'vehicle_preference_questionnaire': {},
            'preferred_vehicles': []
        },
        marketing={
            'lead_direction': '',
            'lead_channel': '',
            'lead_source': '',
            'campaign_medium': '',
            'campaign_source': '',
            'campaign_name': ''
        },

        updated=None,
        created=None,
        reporting_period=None,
        dms_deal={},
        accounting_deal={
            'frontend_gross': {},
            'backend_gross': {},
            'comment': {}
        },
        sales_deal={
            'frontend_gross': {},
            'backend_gross': {},
            'comment': {}
        },
        attachments=[],
        gocard_referral={},
        rdr_punch={},
        finance_checklist={},
        accounting_checklist={},
        extra_checklist={},
        organization_id='',
        alert_types=[],
        test_drive_number=0,
    )

    @property
    def opportunities(self):
        return self.db[OPPORTUNITY]

    @property
    def opportunities_secondary(self):
        return self.db_secondary[OPPORTUNITY]

    def iter_all(self):
        """
        Return an cusor for iterating over all opportunities.
        """
        return self.opportunities.find()

    def create_indexes(self):
        self.opportunities.drop_indexes()
        self.opportunities.create_index(
            [('customer_keywords', 'text'), ('dms_deal.deal_number', 'text')])
        self.opportunities.create_index([('dms_deal.deal_number', 1)])

    def get_opportunities_for_maintenance(self, limit=None,
                                          batch_size=None):
        """ Gets customers by last_maintenance, and updates the datetime.
        :param limit: The limit of customers to retrieve.
        :param batch_size: The size number of records to retrieve per request
        :param distinct: A field of an opportunity to get distinct records on
        """
        cursor = self.iter_all()

        if limit:
            limit = int(limit)
            cursor = cursor.limit(limit)

        if batch_size:
            cursor = cursor.batch_size(batch_size)

        return cursor

    def add_opportunity(self, **kwargs):
        default = self.OPPORTUNITY_DEFAULTS

        opportunity = dict(default, **kwargs)
        opportunity = dict(opportunity, _id=ObjectId())

        now = datetime.utcnow()
        opportunity['created'] = now
        opportunity['updated'] = now
        opportunity['reporting_period'] = reporting_period(now.year, now.month)
        opportunity['last_status_change'] = dict(
            opportunity['last_status_change'],
            **{str(opportunity['status']): now})

        # Ensure we merge default preferences with any provided preferences
        if kwargs.get('preferences') is not None:
            opportunity['preferences'] = dict(default['preferences'], **kwargs['preferences'])

        if not kwargs.get('organization_id'):
            raise TypeError(
                "organization_id is required to create an opportunity")

        if not kwargs.get('dealer_id'):
            raise TypeError("dealer_id is required to create an opportunity")

        self.opportunities.insert_one(opportunity)

        signals.opportunity_created.send(self, opportunity=opportunity)
        return OpportunityModel(opportunity)

    def get_opportunity(self, id):
        match = OpportunitySchema(only=['_id']).load({'_id': id}).data
        opportunity = self.opportunities.find_one(match)
        if opportunity:
            opportunity = OpportunityModel(opportunity)

        return opportunity

    def make_query(self, filters):
        '''
        Given a dict of filters like {'type': value} return
        a mongo query to filter the opportunities collection
        '''
        qry = {'$and': []}

        for filter_type, filter_value in filters.items():
            if filter_type == 'ids':
                f = {'_id': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'statuses':
                f = {'status': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'status_date':
                start_date = filter_value.get('date_from')
                end_date = filter_value.get('date_to')
                date_filter = get_date_filter(start_date, end_date)

                subfilters = []

                for status in OpportunityModel.STATUS.ALL:
                    change_key = 'last_status_change.{}'.format(status)

                    subqry = {
                        'status': status,
                        change_key: date_filter
                    }
                    subfilters.append(subqry)

                qry['$and'].append({"$or": subfilters})

            elif filter_type == 'assignees':
                if 'unassigned' in filter_value:
                    f = {"$and": [
                        {"sales_reps": {"$size": 0}},
                        {"customer_reps": {"$size": 0}},
                        {"sales_managers": {"$size": 0}}
                    ]}
                else:
                    f = {"$or": [
                        {"sales_managers": {"$in": filter_value}},
                        {"sales_reps": {"$in": filter_value}},
                        {"customer_reps": {"$in": filter_value}},
                        {"bdc_reps": {"$in": filter_value}},
                        {"finance_managers": {"$in": filter_value}}
                    ]}
                qry['$and'].append(f)

            elif filter_type == 'bdc_assignees':
                if 'unassigned' in filter_value:
                    f = {"bdc_reps": {"$size": 0}}
                else:
                    f = {"bdc_reps": {"$in": filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'created':
                start_date = filter_value.get('date_from')
                end_date = filter_value.get('date_to')

                date_filter = get_date_filter(start_date, end_date)

                f = {'created': date_filter}
                qry['$and'].append(f)

            elif filter_type == 'updated':
                start_date = filter_value.get('date_from')
                end_date = filter_value.get('date_to')

                date_filter = get_date_filter(start_date, end_date)

                f = {'updated': date_filter}
                qry['$and'].append(f)

            elif filter_type == 'dealer_ids':
                f = {'dealer_id': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'organization_id':
                f = {'organization_id': filter_value}
                qry['$and'].append(f)

            elif filter_type == 'customer_ids':
                customer_ids = filter_value
                if isinstance(customer_ids, basestring):
                    customer_ids = filter_value.split(",")
                customer_ids = map(ObjectId, customer_ids)
                f = {'customer_id': {'$in': customer_ids}}
                qry['$and'].append(f)

            elif filter_type in ['lead_source', 'lead_channel', 'lead_direction']:
                key = 'marketing.{}'.format(filter_type)
                qry['$and'].append({key: filter_value})

            elif filter_type == 'sub_status':
                f = {'sub_status': filter_value}
                qry['$and'].append(f)

            elif filter_type == 'keywords':
                try:
                    terms = [re.compile(t, re.IGNORECASE) for t in shlex.split(filter_value)]
                except (UnicodeEncodeError, ValueError):
                    # Shlex doesnt entirely support unicode and it can't handle single quote inside
                    # Special case to handle `O'rielly`, latin n etc
                    terms = filter_value.split(' ')
                    terms = [re.compile(t, re.IGNORECASE) for t in terms]

                f = {"$or":
                     [{"customer_keywords": {"$in": terms}},
                      {"dms_deal.deal_number": filter_value}]}
                qry['$and'].append(f)

            elif filter_type == 'assigned_to_bdc':
                qry['$and'].append({'bdc_reps.0': {'$exists': filter_value}})

            elif filter_type == 'reporting_period':
                reporting_period = filter_value
                for part in ['year', 'month', 'quarter']:
                    if part in reporting_period:
                        k = 'reporting_period.{}'.format(part)
                        v = reporting_period[part]
                        qry['$and'].append({k: v})

            elif filter_type == 'stock_type':
                f = {'stock_type': filter_value}
                qry['$and'].append(f)

            elif filter_type == 'created_by':
                f = {'creator': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'pitches':
                f = {'pitches': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'leads':
                if not filter_value:
                    f = {'leads': {'$eq': []}}
                else:
                    f = {'leads': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'crm_lead_ids':
                f = {'crm_lead_ids': {'$in': filter_value}}
                qry['$and'].append(f)

            elif filter_type == 'credit_applications':
                f = {'credit_applications': {'$in': filter_value}}
                qry['$and'].append(f)

        if not qry['$and']:
            qry = {}

        return qry

    def _get_opportunities(self, filters, sort_by=None, page=None, page_size=None, filter_query=None):
        query = self.make_query(filters)
        if not query:
            raise ValueError("Invalid query: {}".format(query))
        conditions = []
        conditions.append(query)
        if filter_query:
            conditions.append(filter_query)
        cursor = self.opportunities_secondary.find({'$and': conditions})
        if page and page_size:
            cursor = cursor.skip(page_size * (page - 1)) \
                           .limit(page_size)

        if sort_by:
            cursor.sort(map(lambda x: x.items()[0], sort_by))

        return cursor

    def get_opportunities(self, **kwargs):
        return list(self._get_opportunities(**kwargs))

    def get_opportunities_count(self, **kwargs):
        return self._get_opportunities(**kwargs).count()

    def get_active_opportunities_by_deal_number(self, deal_number, dealer_id=None):
        qry = {'dms_deal.deal_number': deal_number}
        if dealer_id:
            qry['dealer_id'] = dealer_id
            qry['status'] = {
                '$nin': [OpportunityModel.STATUS.LOST, OpportunityModel.STATUS.TUBED]}
        return self.opportunities_secondary.find(qry)

    def get_active_opportunites_by_customer(self, dealer_id, customer_id):
        qry = {'customer_id': customer_id, 'dealer_id': dealer_id}
        qry['status'] = {
            '$nin': [OpportunityModel.STATUS.LOST,
                     OpportunityModel.STATUS.TUBED,
                     OpportunityModel.STATUS.POSTED,
                ]}
        return list(self.opportunities_secondary.find(qry))

    def get_deallog_delivered_by_date(self, dealer_id, date_from, date_to):
        qry = {'dealer_id': dealer_id}
        qry['status'] = {OpportunityModel.STATUS.DELIVERED}
        qry['last_status_change'] = {'status_date':{'date_from':date_from, 'date_to':date_to}}
        return self.opportunities_secondary.find(qry)

    def delete_opportunity(self, id):
        opportunity = self.get_opportunity(id)
        if opportunity:
            match_schema = OpportunitySchema(only=['_id'])
            match = match_schema.load({'_id': id}).data
            self.opportunities.delete_one(match)

            signals.opportunity_deleted.send(self, opportunity=opportunity)
            return True
        return False

    def drop_opportunity_collection(self):
        self.opportunities.delete_many({})

    def update_opportunity_deal_data(self, id, data, field_name):
        if id and data and field_name in ['sales_deal', 'accounting_deal']:
            opportunity = self.get_opportunity(id)
            updated_data = copy.deepcopy(opportunity.get(field_name, {}))
            data['_id'] = ObjectId(id)

            if data.get('comment'):
                comment = data['comment']
                comment['updated'] = datetime.utcnow()

                old_comment = updated_data.get('comment', {})
                merged_comment = dict(old_comment, **comment)
                updated_data['comment'] = merged_comment

            if data.get('frontend_gross'):
                front_gross = data['frontend_gross']
                front_gross['updated'] = datetime.utcnow()

                old_front_gross = updated_data.get('frontend_gross', {})
                merged_front_gross = dict(old_front_gross, **front_gross)
                updated_data['frontend_gross'] = merged_front_gross

            if data.get('backend_gross'):
                back_gross = data['backend_gross']
                back_gross['updated'] = datetime.utcnow()

                old_back_gross = updated_data.get('backend_gross', {})
                merged_back_gross = dict(old_back_gross, **back_gross)
                updated_data['backend_gross'] = merged_back_gross

            if updated_data:
                delta = dictdelta(opportunity, {field_name: updated_data})

                match_schema = OpportunitySchema(only=['_id'])
                match = match_schema.load({'_id': id}).data

                opportunity.update({field_name: updated_data})
                res = self.opportunities.update(
                    match, {"$set": dict(opportunity)})

                signals.opportunity_updated.send(self,
                                                 opportunity=opportunity,
                                                 delta=delta)

            return opportunity

    def update_opportunity(self, id, status_date_change=None, **kwargs):
        if id and kwargs:
            opportunity = self.get_opportunity(id)
            # Check if the status is changing and get the old_status_name.
            if kwargs.get('status') is not None and kwargs['status'] != opportunity.get('status'):
                old_status_name = opportunity.status_name
                if status_date_change is None:
                    status_date_change = datetime.utcnow()

                # Some old opportunities have a Float for a status...
                if type(kwargs['status']) is float:
                    kwargs['status'] = int(kwargs['status'])

                last_status_change = opportunity.setdefault(
                    'last_status_change', {})
                last_status_change[str(kwargs.get('status'))
                                   ] = status_date_change

                # if settings the status to pending and there is no sent to fi date,
                # we want to fill the sent to fi date with the pending date.
                _is_pending = str(kwargs.get('status')) == str(
                    OpportunityModel.STATUS.PENDING)
                _has_fi_date = bool(last_status_change.get(
                    str(OpportunityModel.STATUS.FI)))
                if _is_pending and not _has_fi_date:
                    last_status_change[str(
                        OpportunityModel.STATUS.FI)] = status_date_change

                # Each time an opportunity's status is changed we update the reporting period
                if kwargs.get('reporting_period') is None:
                    kwargs['reporting_period'] = reporting_period(
                        year=datetime.utcnow().year, month=datetime.utcnow().month)

                updated_status = True
            else:
                updated_status = False

            #check if assignment changed from opportunity
            assignee_keys = list(set(kwargs.keys()).intersection(opportunity.assignee_roles))
            if assignee_keys and len(assignee_keys) == 1:
                #if one of assignees has been added or removed, send notifications
                field = { assignee_keys[0]: kwargs[assignee_keys[0]] }
                signals.opportunity_assignment.send(self, opportunity=opportunity, field=field)
                
            # only allow changing dealer id if no dms deal number has been asigned
            if ('dealer_id' in kwargs and
                opportunity.get('dms_deal', {}).get('deal_number') and
                    kwargs['dealer_id'] != opportunity['dealer_id']):
                raise Exception(
                    'Cannot change dealer on an opportunity once it has a DMS deal number')

            # update the dms deal number if one doesnt already exist
            deal_number = kwargs.pop('deal_number', None)
            if deal_number:
                dms_deal = opportunity.setdefault('dms_deal', {})
                if dms_deal.get('deal_number') and dms_deal['deal_number'] != deal_number:
                    raise Exception(
                        "Opportunity already has a DMS deal number assigned!")
                else:
                    dms_deal['deal_number'] = deal_number

            if kwargs.get('reporting_period'):
                opportunity['reporting_period'] = reporting_period(
                    **kwargs['reporting_period'])

            if not kwargs.get('updated'):
                opportunity['updated'] = datetime.utcnow()

            old_sub_status = opportunity.get('sub_status', '')
            delta = dictdelta(opportunity, kwargs)
            opportunity.update(kwargs)

            match_schema = OpportunitySchema(only=['_id'])
            match = match_schema.load({'_id': id}).data
            res = self.opportunities.update(match, {"$set": dict(opportunity)})
            signals.opportunity_updated.send(self, opportunity=opportunity,
                                             delta=delta)

            # Send the updated status with the old_status_name.
            if updated_status:
                signals.opportunity_status_updated.send(self,
                                                        opportunity=opportunity,
                                                        old_opportunity_status_name=old_status_name)
            # Send signal if sub_status has changed
            if old_sub_status != opportunity.get('sub_status', ''):
                signals.opportunity_sub_status_updated.send(self, opportunity=opportunity)

            return opportunity
        else:
            raise Exception('No data provided, or invalid arguments')

    def merge_customer_opportunities(self, merge_customer, source_customers):
        """
        Transfer all opportunities from the source customers to the merge customer.
        :param merge_customer: Customer, merge user
        :param source_customers: List of Customers
        """
        source_customer_ids = [c['_id'] for c in source_customers]
        query = {'customer_id': {'$in': source_customer_ids}}
        update = {'$set': {'customer_id': merge_customer['_id']}}
        self.opportunities.update(query, update, multi=True)

    def edit_deal_number(self, id, deal_number):
        """
        MPDESK-1376 Temporary functionality to edit deal number?

        Attempt to pull from CDK first and if it fails don't update
        the deal number on the opportunity.

        :param id: string, Opportunity ID
        :param deal_number: string, deal number value
        :return: updated opportunity
        """
        # Try updating the dms_deal from CDK
        opportunity = self.get_opportunity(id)

        from market_crm import dap
        deal_host_item_id = 'FI-WIP*{}'.format(deal_number)

        try:
            dap.handle_vehicle_sale(
                opportunity['dealer_id'], 'VehicleSales', deal_host_item_id)

            dms_deal = {'deal_number': deal_number}
            opportunity['dms_deal'] = dms_deal
            self.update_opportunity(id, dms_deal=dms_deal)

            signals.opportunity_updated.send(
                self, opportunity=opportunity, delta={'dms_deal': dms_deal})

            opportunity = self.get_opportunity(id)
        except:
            return None

        return opportunity


    def update_dms_deal(self, id, deal_data):
        opportunity = self.get_opportunity(id)
        dms_deal = opportunity['dms_deal']

        if not dms_deal.get('deal_number'):
            raise Exception(
                "Cant update deal data because opportunity has no DMS deal number!")

        assert 'deal_number' not in deal_data

        dms_deal.update(deal_data)
        stock_type = (dms_deal.get('deal_type') or '').lower()
        if stock_type not in OpportunityStockTypeOptions.ALL:
            stock_type = OpportunityStockTypeOptions.UNKNOWN

        self.update_opportunity(id, dms_deal=dms_deal, stock_type=stock_type)

        signals.opportunity_updated.send(
            self, opportunity=opportunity, delta={'dms_deal': deal_data})
        return opportunity

    def add_attachment(self, opportunity_id, attachment_type, key, **kwargs):
        """
        Add an Attachment to an opportunity
        :param opportunity_id: Opportunity object ID
        :param attachment_type: One of Opportunity.ATTACHMENT_TYPE
        :param key: Amazon S3 Key
        :param label: Attachment filename label
        :param created_by: Username of attachment uploader
        :param created_by_name: Display name of attachment uploader
        :param file_hash: Attachment file hash
        :param content_type: Attachment content type
        :param file_tag: User specified file type/tag/kind
        :return: Updated Opportunity
        """
        opportunity = self.get_opportunity(opportunity_id)

        if opportunity:
            attachment_id = ObjectId()

            attachment = {
                '_id': attachment_id,
                'attachment_type': attachment_type,
                'key': key,
                'label': kwargs.get('label'),
                'created_by': kwargs.get('created_by'),
                'created_by_name': kwargs.get('created_by_name'),
                'file_hash': kwargs.get('file_hash'),
                'file_size': kwargs.get('file_size'),
                'content_type': kwargs.get('content_type'),
                'file_tag': kwargs.get('file_tag'),
                'date_created': datetime.utcnow(),
                'deleted': False,
            }

            attachments = opportunity.get('attachments', [])
            opportunity['attachments'] = attachments + [attachment]
            self.update_opportunity(id=opportunity_id, **opportunity)

        return opportunity

    def modify_attachment(self, opportunity_id, attachment_id, **kwargs):
        """
        Modify an attachment on the opportunity
        :param attachment_id: string id of the Attachment to modify
        :return: Updated Opportunity
        """
        opportunity_id = ObjectId(opportunity_id)
        attachment_id = ObjectId(attachment_id)

        # Find the opportunity with matching attachment_id
        opportunity = self.opportunities.find_one({
            '_id': opportunity_id,
            'attachments': {
                '$elemMatch': {
                    '_id': attachment_id
                }
            }
        })

        if opportunity:
            for attachment in opportunity.get('attachments'):
                if attachment.get('_id') == attachment_id:
                    attachment.update(kwargs)
                    self.update_opportunity(opportunity_id, **opportunity)

        return opportunity

    def remove_attachment(self, opportunity_id, attachment_id):
        """
        Remove an attachment from an opportunity
        :param attachment_id: string id of the Attachment to remove
        :return: Updated Opportunity
        """
        opportunity_id = ObjectId(opportunity_id)
        attachment_id = ObjectId(attachment_id)

        update = {'deleted': True}

        return self.modify_attachment(opportunity_id, attachment_id, **update)

    def update_preferences(self, id, **kwargs):
        opportunity = self.get_opportunity(id)
        if opportunity:
            if not opportunity.get('preferences'):
                opportunity['preferences'] = dict(self.OPPORTUNITY_DEFAULTS['preferences'])

            for k, v in kwargs.items():
                opportunity['preferences'][k] = v

            opportunity = self.update_opportunity(
                id, preferences=opportunity['preferences'])

        return opportunity

    def get_preferences(self, id):
        opportunity = self.get_opportunity(id)
        return opportunity['preferences']

    def update_marketing_data(self, id, **kwargs):
        opportunity = self.get_opportunity(id)

        if opportunity:
            for k, v in kwargs.items():
                opportunity['marketing'][k] = v
            opportunity = self.update_opportunity(
                id, marketing=opportunity['marketing'])

        return opportunity

    def get_marketing_data(self, id):
        opportunity = self.get_opportunity(id)
        if opportunity:
            return opportunity['marketing']
        return opportunity

    def update_opportunities_for_customer(self, customer, delta=None):
        '''
        :param customer: A customer document
        '''
        customer_keyword_fields = [
            'first_name',
            'last_name',
            'company_name',
            'drivers_license',
            'phone',
            'work_phone',
            'cell_phone',
            'home_phone',
        ]

        if delta is not None:
            # we can check the delta to see if this is necessary.
            # If the delta does not contain any of out keyword fields
            # we can exit early.
            if not any(delta.get(f) for f in customer_keyword_fields):
                return

        qry = {
            'customer_id': customer['_id']
        }

        customer_name = u'{} {}'.format(customer.get('first_name', ''),
                                        customer.get('last_name', ''))

        keywords = [customer.get(f)
                    for f in customer_keyword_fields] + [customer_name]
        keywords = filter(bool, keywords)  # remove empty values
        for email in customer.get('emails') or []:
            keywords.append(email['email'])

        update = {'$set': {
            'customer_name': customer_name.strip(),
            'customer_keywords': keywords
        }}
        self.opportunities.update(qry, update, multi=True)

    def update_opportunities_with_dealer_name(self, dealer_id):
        '''
        :param dealer_id: The dealership id
        '''

        qry = {
            'dealer_id': dealer_id
        }

        dealer = dealer_name(dealer_id)

        update = {'$set': {
            'dealer_name': dealer
        }}

        self.opportunities.update(qry, update, multi=True)

    def update_opportunity_with_dealer_name(self, opportunity):
        '''
        :param opportunity: The opportunity to be updated
        '''

        qry = {
            '_id': opportunity['_id'],
            'dealer_id': opportunity['dealer_id']
        }

        dealer = dealer_name(opportunity['dealer_id'])

        update = {'$set': {
            'dealer_name': dealer
        }}

        self.opportunities.update(qry, update)

    def set_reporting_period(self, opportunity_id, year, month):
        '''
        set the reporting period for an opportunity.
        '''
        opportunity = self.update_opportunity(
            opportunity_id,
            reporting_period=reporting_period(year=year, month=month))
        return opportunity

    def aggregate_opportunity_data_by_dealer(self, organization_id, dealer_ids, created):
        carryover_value = 0
        open_status_filter = []
        start_date = created['date_from']
        end_date = created['date_to'] or datetime.utcnow()
        end_date = end_date + timedelta(days=1)

        closed_status_filter = {'$and': [
            {'status': {'$in': OpportunityModel.STATUS.CLOSED}},
            {'created': {
                '$gte': start_date,
                '$lt': end_date
            }}
        ]}

        created_date_filters = [
            {'$gte': ['$created', start_date]},
            {'$lt': ['$created', end_date]}
        ]

        # Any open opportunities are always carried over to the current month, so on the month-to-date
        # view, we include all open opportunities. This is the same logic used in the deal log
        if start_date.month == datetime.utcnow().month and start_date.year == datetime.utcnow().year:
            carryover_value = 1
            open_status_filter.append(
                {'status': {'$in': OpportunityModel.STATUS.OPEN}})

        # Select oppourtunity documents based off of dealer and status
        match = {
            '$match': {
                '$and': [
                    {'organization_id': organization_id},
                    {'dealer_id': {'$in': dealer_ids}},
                    {'$or': [closed_status_filter] + open_status_filter}
                ]
            }
        }

        IS_OPEN = {'$setIsSubset': [['$status'], OpportunityModel.STATUS.OPEN]}

        # Unassigned Opportunities are of status OPEN with no
        # sales rep or customer rep or sales manager
        IS_UNASSIGNED = {
            '$and': [
                IS_OPEN,
                {'$eq': [{'$size': '$sales_reps'}, 0]},
                {'$eq': [{'$size': '$customer_reps'}, 0]},
                {'$eq': [{'$size': '$sales_managers'}, 0]},
            ]
        }

        # Project new summable fields based off of conditionals
        project = {
            '$project': {
                'dealer_id': 1,
                'is_open': {'$cond': [IS_OPEN, 1, 0]},
                'is_carryover': {'$cond': [{'$lt': ['$created', start_date]}, carryover_value, 0]},
                'is_unassigned': {'$cond': [IS_UNASSIGNED, 1, 0]},
                'is_this_period': {'$cond': [{'$and': created_date_filters}, 1, 0]},
                'inbound_web': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "web"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_phone': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "phone"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_walk': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "walk"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_chat': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "chat"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_sms': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "sms"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_email': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "email"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_event': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "event"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_social': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "social"]}
                    ] + created_date_filters}, 1, 0]},
                'inbound_service': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "service"]}
                    ] + created_date_filters}, 1, 0]},
                'outbound_phone': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "outbound"]},
                        {'$eq': ['$marketing.lead_channel', "phone"]}
                    ] + created_date_filters}, 1, 0]},
                'outbound_sms': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "outbound"]},
                        {'$eq': ['$marketing.lead_channel', "sms"]}
                    ] + created_date_filters}, 1, 0]},
                'outbound_email': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "outbound"]},
                        {'$eq': ['$marketing.lead_channel', "email"]}
                    ] + created_date_filters}, 1, 0]}
            }
        }

        # Group all documents by dealer_id and sum the results
        group = {
            '$group': {
                '_id': {'dealer_id': '$dealer_id'},
                'opportunity_ids': {'$addToSet': '$_id'},
                'total_opportunities': {'$sum': 1},
                'total_open': {'$sum': '$is_open'},
                'total_carryover': {'$sum': '$is_carryover'},
                'total_this_period': {'$sum': '$is_this_period'},
                'total_unassigned': {'$sum': '$is_unassigned'},
                'total_inbound_web': {'$sum': '$inbound_web'},
                'total_inbound_phone': {'$sum': '$inbound_phone'},
                'total_inbound_walk': {'$sum': '$inbound_walk'},
                'total_inbound_chat': {'$sum': '$inbound_chat'},
                'total_inbound_sms': {'$sum': '$inbound_sms'},
                'total_inbound_email': {'$sum': '$inbound_email'},
                'total_inbound_event': {'$sum': '$inbound_event'},
                'total_inbound_social': {'$sum': '$inbound_social'},
                'total_inbound_service': {'$sum': '$inbound_service'},
                'total_outbound_phone': {'$sum': '$outbound_phone'},
                'total_outbound_sms': {'$sum': '$outbound_sms'},
                'total_outbound_email': {'$sum': '$outbound_email'},
            }
        }

        data = self.opportunities_secondary.aggregate([match, project, group])
        return list(data)

    def aggregate_opportunity_assignees(self, filters):
        match = {'$match': self.make_query(filters)}

        project = {
            '$project': {
                'assignees': {
                    '$setUnion': ['$sales_reps', '$sales_managers', '$bdc_reps', '$finance_managers', '$customer_reps']
                }
            }
        }

        unwind = {'$unwind': {'path': '$assignees'}}

        group = {
            '$group': {'_id': None, 'assignees': {'$addToSet': '$assignees'}}
        }

        data = self.opportunities_secondary.aggregate(
            [match, project, unwind, group])
        return list(data)

    def aggregate_opportunity_sales_funnel_reports(self, filters):
        match = {'$match': self.make_query(filters)}

        # Project new summable fields based off of conditionals
        project = {
            '$project': {
                'dealer_id': 1,
                'is_fresh': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.FRESH]}, 1, 0
                ]},
                'is_desk': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.DESK]}, 1, 0
                ]},
                'is_fi': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.FI]}, 1, 0
                ]},
                'is_posted': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.POSTED]}, 1, 0
                ]},
                'is_delivered': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.DELIVERED]}, 1, 0
                ]},
                'is_lost': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.LOST]}, 1, 0
                ]},
                'is_pending': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.PENDING]}, 1, 0
                ]},
                'is_approved': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.APPROVED]}, 1, 0
                ]},
                'is_signed': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.SIGNED]}, 1, 0
                ]},
                'is_tubed': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.TUBED]}, 1, 0
                ]},
                'is_carryover': {'$cond': [
                    {'$eq': ['$status', OpportunityModel.STATUS.CARRYOVER]}, 1, 0
                ]},
                'total_gross': {'$ifNull': ['$dms_deal.total_gross', 0]}
            }
        }

        # Group all documents by dealer_id and sum the results
        group = {
            '$group': {
                '_id': {'dealer_id': '$dealer_id'},
                'total_opportunities': {'$sum': 1},
                'total_fresh': {'$sum': '$is_fresh'},
                'total_desk': {'$sum': '$is_desk'},
                'total_fi': {'$sum': '$is_fi'},
                'total_posted': {'$sum': '$is_posted'},
                'total_delivered': {'$sum': '$is_delivered'},
                'total_lost': {'$sum': '$is_lost'},
                'total_pending': {'$sum': '$is_pending'},
                'total_approved': {'$sum': '$is_approved'},
                'total_signed': {'$sum': '$is_signed'},
                'total_tubed': {'$sum': '$is_tubed'},
                'total_carryover': {'$sum': '$is_carryover'},
                'total_gross': {'$sum': '$total_gross'}
            }
        }

        data = self.opportunities_secondary.aggregate([match, project, group])
        return list(data)

    def aggregate_deallog_recap_reports(self, filters):
        match = {'$match': self.make_query(filters)}

        # Project new summable fields based off of conditionals
        project = {
            '$project': {
                'dealer_id': 1,
                'is_done': {'$cond': [
                    {'$and': [
                        {'$ne': ['$status', OpportunityModel.STATUS.DELIVERED]}
                    ]}, 1, 0]
                },
                'is_delivered': {'$cond': [
                    {'$and': [
                        {'$eq': ['$status', OpportunityModel.STATUS.DELIVERED]}
                    ]}, 1, 0]
                },
                'frontend_gross': {'$ifNull': ['$dms_deal.frontend_gross', 0]},
                'backend_gross': {'$ifNull': ['$dms_deal.backend_gross', 0]},
                'total_gross': {'$ifNull': ['$dms_deal.total_gross', 0]}
            }
        }

        # Group all documents by dealer_id and sum the results
        group = {
            '$group': {
                '_id': {
                    'dealer_id': '$dealer_id',
                },
                'opportunity_ids': {'$addToSet': '$_id'},
                'total_opportunities': {'$sum': 1},
                'total_deal_done': {'$sum': '$is_done'},
                'total_deal_delivered': {'$sum': '$is_delivered'},
                'total_gross': {'$sum': '$total_gross'},
                'total_frontgross': {'$sum': '$frontend_gross'},
                'total_endgross': {'$sum': '$backend_gross'}
            }
        }

        data = self.opportunities_secondary.aggregate([match, project, group])
        return list(data)

    def aggregate_daily_operations_reports(self, filters):
        match = {'$match': self.make_query(filters)}

        # Project new summable fields based off of conditionals
        project = {
            '$project': {
                'dealer_id': 1,
                'deal_type': {'$ifNull': ['$dms_deal.deal_type', 'Unknown']},
                'is_pending': {'$cond': [
                    {'$or': [
                        {'$eq': ['$status', OpportunityModel.STATUS.APPROVED]},
                        {'$eq': ['$status', OpportunityModel.STATUS.PENDING]},
                        {'$eq': ['$status', OpportunityModel.STATUS.SIGNED]}
                    ]}, 1, 0]
                },
                'is_sold': {'$cond': [
                    {'$or': [
                        {'$eq': ['$status', OpportunityModel.STATUS.DELIVERED]},
                        {'$eq': ['$status', OpportunityModel.STATUS.POSTED]},
                    ]}, 1, 0]
                },
                'total_gross': {'$ifNull': ['$dms_deal.total_gross', 0]}
            }
        }

        # Group all documents by dealer_id and sum the results
        group = {
            '$group': {
                '_id': {
                    'dealer_id': '$dealer_id',
                    'deal_type': '$deal_type'
                },
                'total_opportunities': {'$sum': 1},
                'total_pending_for_deal_type': {'$sum': '$is_pending'},
                'total_sold_for_deal_type': {'$sum': '$is_sold'},
                'total_gross_for_deal_type': {'$sum': '$total_gross'}
            }
        }

        data = self.opportunities_secondary.aggregate([match, project, group])
        return list(data)

    def aggregate_h2h_opportunity_leads_report_data(self, filters):
        match = {'$match': self.make_query(filters)}

        project = {
            '$project': {
                '_id': 1,
                'bdc_reps': 1,
                'inbound_service': {'$cond': [
                    {'$and': [
                        {'$eq': ['$marketing.lead_direction', "inbound"]},
                        {'$eq': ['$marketing.lead_channel', "service"]}
                    ]}, 1, 0]}
            }
        }

        unwind = {'$unwind': {'path': '$bdc_reps'}}

        group = {
            '$group': {
                '_id': {'bdc_rep': '$bdc_reps'},
                'total_opportunities': {'$sum': 1},
                'inbound_service': {'$sum': '$inbound_service'}
            }
        }

        data = self.opportunities_secondary.aggregate(
            [match, project, unwind, group])
        return list(data)

    def aggregate_h2h_opportunity_delivered_report_data(self, filters):
        match = {'$match': self.make_query(filters)}

        project = {
            '$project': {
                '_id': 1,
                'bdc_reps': 1,
                'full_sale_delivered': {'$cond': [
                    {'$and': [
                        {'$eq': [{'$size': '$bdc_reps'}, 1]},
                        {'$eq': ['$status', OpportunityModel.STATUS.DELIVERED]}
                    ]}, 1, 0]},
                'half_sale_delivered': {'$cond': [
                    {'$and': [
                        {'$gt': [{'$size': '$bdc_reps'}, 1]},
                        {'$eq': ['$status', OpportunityModel.STATUS.DELIVERED]}
                    ]}, 1, 0]},
                'full_sale_posted': {'$cond': [
                    {'$and': [
                        {'$eq': [{'$size': '$bdc_reps'}, 1]},
                        {'$eq': ['$status', OpportunityModel.STATUS.POSTED]}
                    ]}, 1, 0]},
                'half_sale_posted': {'$cond': [
                    {'$and': [
                        {'$gt': [{'$size': '$bdc_reps'}, 1]},
                        {'$eq': ['$status', OpportunityModel.STATUS.POSTED]}
                    ]}, 1, 0]}
            }
        }

        unwind = {'$unwind': {'path': '$bdc_reps'}}

        group = {
            '$group': {
                '_id': {'bdc_rep': '$bdc_reps'},
                'full_sale_delivered': {'$sum': '$full_sale_delivered'},
                'half_sale_delivered': {'$sum': '$half_sale_delivered'},
                'full_sale_posted': {'$sum': '$full_sale_posted'},
                'half_sale_posted': {'$sum': '$half_sale_posted'}
            }
        }

        data = self.opportunities_secondary.aggregate(
            [match, project, unwind, group])
        return list(data)

    def aggregate_dealership_status_report(self, filters):
        match = {'$match': self.make_query(filters)}

        project = {
            '$project': {
                'dealer_id': 1,
                'credit_applications': 1,
                'chat': {'$cond': [
                    {'$eq': ['$marketing.lead_channel', "chat"]}, 1, 0]},
                'phone': {'$cond': [
                    {'$eq': ['$marketing.lead_channel', "phone"]}, 1, 0]},
                'email': {'$cond': [
                    {'$eq': ['$marketing.lead_channel', "email"]}, 1, 0]},
                'sms': {'$cond': [
                    {'$eq': ['$marketing.lead_channel', "sms"]}, 1, 0]},
                'completed': {'$cond': [
                    {'$setIsSubset': [['$status'], OpportunityModel.STATUS.COMPLETED]}, 1, 0]}
            }
        }

        group = {
            '$group': {
                '_id': {'dealer_id': '$dealer_id'},
                'opportunity_ids': {'$addToSet': '$_id'},
                'credit_applications': {'$addToSet': '$credit_applications'},
                'total_chat': {'$sum': '$chat'},
                'total_phone': {'$sum': '$phone'},
                'total_email': {'$sum': '$email'},
                'total_sms': {'$sum': '$sms'},
                'total_completed': {'$sum': '$completed'},
                'total_count': {'$sum': 1},
            }
        }

        data = self.opportunities_secondary.aggregate([match, project, group])
        return list(data)

    def aggregate_employee_opportunity_report(self, filters):
        match = {'$match': self.make_query(filters)}
        # Join the customer information with the opportunity data
        lookup = {
            '$lookup': {
                'from': 'customer',
                'localField': 'customer_id',
                'foreignField': '_id',
                'as': 'customer'
            }
        }

        # Find size of all relevant user phones and emails
        project = {
            '$project': {
                'creator': 1,
                'customer_phones': {
                    '$size': {
                        '$filter': {
                            'input': ['$customer.cell_phone', '$customer.work_phone',
                                      '$customer.home_phone', '$customer.phone'],
                            'as': 'phone',
                            'cond': {'$and': [
                                {'$ne': ['$$phone', [None]]},
                                {'$ne': ['$$phone', ['None']]}
                            ]}
                        }
                    }
                },
                'customer_emails': {
                    '$size': {
                        '$filter': {
                            'input': '$customer.emails',
                            'as': 'email',
                            'cond': {'$and': [
                                {'$ne': ['$$email', []]},
                                {'$ne': ['$$email', [None]]},
                            ]}
                        }
                    }
                }
            }
        }

        # Given integer value for phones and emails, reduce to 1 or 0 for sum calc
        project_reduce = {
            '$project': {
                'creator': 1,
                'customer_phones': {
                    '$cond': {
                        'if': {'$gt': ['$customer_phones', 0]},
                        'then': 1,
                        'else': 0
                    }
                },
                'customer_emails': {
                    '$cond': {
                        'if': {'$gt': ['$customer_emails', 0]},
                        'then': 1,
                        'else': 0
                    }
                }
            }
        }

        # Group all customer values per staff member (creator)
        group = {
            '$group': {
                '_id': {
                    'creator': '$creator',
                },
                'customer_phones': {
                    '$sum': '$customer_phones',
                },
                'customer_emails': {
                    '$sum': '$customer_emails',
                },
                'customer_phones_and_emails': {
                    '$sum': {
                        '$min': ['$customer_phones', '$customer_emails']
                    }
                },
                'opportunities': {'$sum': 1},
            }
        }

        data = self.opportunities_secondary.aggregate(
            [match, lookup, project, project_reduce, group]
        )
        return list(data)
