import { Injectable, Inject, NotFoundException, NotAcceptableException, BadRequestException, Logger } from '@nestjs/common';
import { Notifications } from '../../legacy_models/entities/Notifications';
import {
    CreateNotificationDto,
    CheckoutNotificationDto,
    CreditAppNotificationDto,
    CustomerSavedSearchNotificationDto,
    SalesSavedSearchNotificationDto
} from './dto/notification.dto'
import { Repository, getManager } from 'typeorm';
import { isEmpty } from 'lodash';
import {
    Topics, CUSTOMER_SUCCESSFUL_CHECKOUT,
    SALES_SUCCESSFUL_CHECKOUT,
    CUSTOMER_COMPLETED_CREDIT_APP,
    SALES_COMPLETED_CREDIT_APP,
    CUSTOMER_SAVED_SEARCH,
    SALES_SAVED_SEARCH
} from './topic';
import { DELIVERY_TYPE, SECRET_PASSPHRASE } from '../common/utils/constants';
import { renderEmail } from '../common/utils/helper';
import { HandlerFactory } from './handler';
import { VehicleInvs } from '../../legacy_models/entities/VehicleInvs';
import { VehicleInvsService } from '../vehicle-invs/vehicle-invs.service';
import { ExceptionHandler } from 'winston';
import { ENV_PARAMS } from "../environment";
import { formatSearch } from "../customer/saved-vehicle-searches/helper";
const logger = require('../logger');

@Injectable()
export class NotificationService {
    recipients: Array<string>;
    subject: string;
    bodyText: string;
    bodyHtml: string;

    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
    ) { }

    async create(data: object): Promise<number[]> {
        const { recipients, subject, topic, bodyHtml, bodyText, onClickUrl = '' } = <CreateNotificationDto>data;

        if ([recipients, subject].some(item => item == null))
            throw new Error('invalid inputs');

        const ids = [];
        for (const recipient of recipients) {
            const notification = await getManager().insert(Notifications, { recipient, topic, subject, bodyText, bodyHtml, onClickUrl });
            const id = notification.generatedMaps[0]['id'] || null;
            this.send(id);
            ids.push(id);
        }

        return ids;
    }

    async send(id: number) {
        try {
            const notification = await this.notificationResitory.findOne(id);
            const handler = HandlerFactory(DELIVERY_TYPE.EMAIL, notification);
            await handler.notify();
        }
        catch (error) {
            logger.error(error);
        }
    }

    getAllVehicleCount(results: Array<object>): number {
        if (results.length === 0)
            return 0;

        const vehicleCount = results.reduce((pre, v) => pre += v['vehicles'].length, 0);
        return vehicleCount;
    }

    getUnsubscribeUrl(email: string = null): string {
        if (email === null)
            return;

        const CryptoJS = require("cryptojs.js");
        const encryptedEmail = CryptoJS.encrypt(email, SECRET_PASSPHRASE);
        return `${ENV_PARAMS['WEB_APP_HOST']}/se/delete-saved-search?email=${encryptedEmail}`;
    }

    numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    getOnSearchUrl(filters: object = null): string {
        if (!filters)
            return '';

        let url = new URL(`${ENV_PARAMS['WEB_APP_HOST']}/cars`);

        if (filters['makes'] && filters['makes'].length > 0) {
            url = new URL(`${ENV_PARAMS['WEB_APP_HOST']}/cars/${filters['makes'].join('+')}`);
        }
        if (filters['bodyTypes'] && filters['bodyTypes'].length > 0) {
            url.searchParams.append('type', `${filters['bodyTypes'].join('+')}`);
        }
        if (filters['maxPrice'] !== null) {
            const minPrice = !isEmpty(filters['minPrice']) ? filters['minPrice'] : 0;
            url.searchParams.append('price', `${minPrice}-${filters['maxPrice']}`);
        }
        if (filters['minYear'] !== null && filters['maxYear'] !== null) {
            url.searchParams.append('year', `${filters['minYear']}-${filters['maxYear']}`);
        }
        if (filters['maxMileage'] !== null) {
            const minMileage = !isEmpty(filters['minMileage']) ? filters['minMileage'] : 0;
            url.searchParams.append('mileage', `${minMileage}-${filters['maxMileage']}`);
        }
        if (filters['transmissionTypes'] && filters['transmissionTypes'].length > 0) {
            url.searchParams.append('transmission', `${filters['transmissionTypes'].join('+')}`);
        }

        return url.toString();
    }

    searchedFilterToString(filters: object = null): string {
        if (filters === null)
            return;

        return formatSearch([filters])[0]['searchText'] || '';
    }
}

@Injectable()
export class CustomerCheckoutNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
        private vehicleInvsService: VehicleInvsService,
    ) {
        super(notificationResitory);
    }

    async create(data: CheckoutNotificationDto): Promise<number[]> {
        const { firstName = null, lastName = null, email = null, vehicleID = null, onClickUrl = null } = data;
        if ([firstName, lastName, email, vehicleID].some(item => item === null))
            throw new BadRequestException;

        try {
            const vehicle = await this.vehicleInvsService.getOne(vehicleID);
            const topic = Topics.find(item => item.topic === CUSTOMER_SUCCESSFUL_CHECKOUT);

            const bodyHtml = await renderEmail({
                firstName, lastName, vehicle, onClickUrl
            }, topic.template);

            return await super.create({ topic: CUSTOMER_SUCCESSFUL_CHECKOUT, subject: topic.subject, bodyHtml, bodyText: '', recipients: [email] });
        }
        catch (error) {
            logger.error('sending checkout notification error:' + error);
        }

    }
}

@Injectable()
export class SalesCheckoutNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
        private vehicleInvsService: VehicleInvsService,
    ) {
        super(notificationResitory);
    }

    async create(data: CheckoutNotificationDto): Promise<number[]> {
        const { firstName = null, lastName = null, vehicleID = null } = data;
        if ([firstName, lastName, vehicleID].some(item => item === null))
            throw new BadRequestException;

        try {
            const emails = [ENV_PARAMS['SALES_EMAIL_ADDRESS']];
            const vehicle = await this.vehicleInvsService.getOne(vehicleID);
            const topic = Topics.find(item => item.topic === SALES_SUCCESSFUL_CHECKOUT);
            const bodyHtml = await renderEmail({
                firstName, lastName, vehicle
            }, topic.template);

            return await super.create({ topic: SALES_SUCCESSFUL_CHECKOUT, subject: topic.subject, bodyHtml, bodyText: '', recipients: [...emails] });
        }
        catch (error) {
            logger.error('sending checkout notification error:' + error);
        }

    }
}

@Injectable()
export class CustomerCompletedCreditAppNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
        private vehicleInvsService: VehicleInvsService,
    ) {
        super(notificationResitory);
    }

    async create(data: CreditAppNotificationDto): Promise<number[]> {
        const { firstName = null, lastName = null, email = null } = data;
        if ([{ firstName, lastName, email }].some(item => item === null))
            throw new BadRequestException;

        try {
            const topic = Topics.find(item => item.topic === CUSTOMER_COMPLETED_CREDIT_APP);

            const bodyHtml = await renderEmail({
                firstName, lastName
            }, topic.template);

            return await super.create({ topic: CUSTOMER_SUCCESSFUL_CHECKOUT, subject: topic.subject, bodyHtml, bodyText: '', recipients: [email] });
        }
        catch (error) {
            logger.error('sending customer credit ap notification error:' + error);
            throw new Error(error);
        }
    }
}

@Injectable()
export class SalesCompletedCreditAppNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
        private vehicleInvsService: VehicleInvsService,
    ) {
        super(notificationResitory);
    }

    async create(data: CreditAppNotificationDto): Promise<number[]> {
        const { firstName = null, lastName = null } = data;

        if ([firstName, lastName].some(item => item === null))
            throw new BadRequestException;

        try {
            const topic = Topics.find(item => item.topic === SALES_COMPLETED_CREDIT_APP);
            const emails = [ENV_PARAMS['SALES_EMAIL_ADDRESS']];
            const bodyHtml = await renderEmail({
                firstName, lastName
            }, topic.template);

            return await super.create({ topic: SALES_COMPLETED_CREDIT_APP, subject: topic.subject, bodyHtml, bodyText: '', recipients: [...emails] });
        }
        catch (error) {
            logger.error('sending sales credit ap notification error:' + error);
            throw new Error(error);
        }
    }
}

@Injectable()
export class SavedSearchesNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
    ) {
        super(notificationResitory);
    }

    async create(data: CustomerSavedSearchNotificationDto): Promise<number[]> {
        let { customerName = null, email = null, results = [], uniqueVehicles = 0 } = data;

        try {
            for (let result of results) {
                result['search_url'] = this.getOnSearchUrl({ ...result['search_filters'] });
                result['search_filters'] = this.searchedFilterToString(result['search_filters'] ?? null);
                for (let vehicle of result['vehicles']) {
                    vehicle['kilometers'] = this.numberWithCommas(vehicle['kilometers']);
                }
            }

            const topic = Topics.find(item => item.topic === CUSTOMER_SAVED_SEARCH);
            const bodyHtml = await renderEmail({
                name: customerName,
                results, unsubscribe_url: this.getUnsubscribeUrl(email), uniqueVehicles
            }, topic.template);

            return await super.create({ topic: CUSTOMER_SAVED_SEARCH, subject: topic.subject, bodyHtml, bodyText: '', recipients: [email] });
        }
        catch (error) {
            logger.error('sending saved searches notification error:' + error);
            throw new Error(error);
        }
    }

}

@Injectable()
export class SavedSearchesForSalesNotificationService extends NotificationService {
    constructor(
        @Inject('NOTIFICATION_REPOSITORY')
        protected notificationResitory: Repository<Notifications>,
    ) {
        super(notificationResitory);
    }

    async create(data: SalesSavedSearchNotificationDto): Promise<number[]> {
        let { email: customerEmail = null, customerName = null, search_filters = null } = data;
        let email = ENV_PARAMS['SALES_EMAIL_ADDRESS'];
        try {
            search_filters['search_filters'] = this.searchedFilterToString(search_filters);

            const topic = Topics.find(item => item.topic === SALES_SAVED_SEARCH);
            const bodyHtml = await renderEmail({
                customerEmail,
                name: customerName,
                search_filters,
            }, topic.template);

            return await super.create({ topic: SALES_SAVED_SEARCH, subject: topic.subject, bodyHtml, bodyText: '', recipients: [email] });
        }
        catch (error) {
            logger.error('sending saved searches notification error:' + error);
            throw new Error(error);
        }
    }
}
