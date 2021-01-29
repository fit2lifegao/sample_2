import { Body, Controller, Get, Param, Post, Put, ParseIntPipe, HttpStatus, NotFoundException } from '@nestjs/common';
import {
  NotificationService,
  CustomerCheckoutNotificationService,
  SalesCheckoutNotificationService,
  CustomerCompletedCreditAppNotificationService,
  SalesCompletedCreditAppNotificationService,
  SavedSearchesNotificationService,
  SavedSearchesForSalesNotificationService,
} from "./notification.service"
import {
  Topics, CUSTOMER_SUCCESSFUL_CHECKOUT,
  SALES_SUCCESSFUL_CHECKOUT,
  CUSTOMER_COMPLETED_CREDIT_APP,
  SALES_COMPLETED_CREDIT_APP
} from './topic';
import { 
  CreateNotificationDto, 
  CheckoutNotificationDto, 
  CreditAppNotificationDto,
  CustomerSavedSearchNotificationDto,
  SalesSavedSearchNotificationDto,
 } from './dto/notification.dto'

@Controller('notification')
export class NotificationController {
  constructor(
    private readonly notificationService: NotificationService,
    private readonly customerCheckoutNotificationService: CustomerCheckoutNotificationService,
    private readonly salesCheckoutNotificationService: SalesCheckoutNotificationService,
    private readonly customerCompletedCreditAppNotificationService: CustomerCompletedCreditAppNotificationService,
    private readonly salesCompletedCreditAppNotificationService: SalesCompletedCreditAppNotificationService,
    private readonly savedSearchesNotificationService: SavedSearchesNotificationService,
    private readonly savedSearchesForSalesNotificationService: SavedSearchesForSalesNotificationService,
  ) { }

  @Post('create')
  async create(@Body() data: CreateNotificationDto) {
    const result = await this.notificationService.create(data);
    return result;
  }

  @Post('create/customer_successful_checkout')
  async createCustomerSuccessfulCheckout(@Body() data: CheckoutNotificationDto) {
    const result = await this.customerCheckoutNotificationService.create(data);
    return result;
  }

  @Post('create/sales_successful_checkout')
  async createSaleSuccessfulCheckout(@Body() data: CheckoutNotificationDto) {
    const result = await this.salesCheckoutNotificationService.create(data);
    return result;
  }


  @Post('create/customer_completed_credit_app')
  async createCustomerCompleteCreditApp(@Body() data: CreditAppNotificationDto) {
    const result = await this.customerCompletedCreditAppNotificationService.create(data);
    return result;
  }


  @Post('create/sales_completed_credit_app')
  async createCuscreateSalesCompleteCreditApptomerSuccessfulCheckout(@Body() data: CreditAppNotificationDto) {
    const result = await this.salesCompletedCreditAppNotificationService.create(data);
    return result;
  }

  @Post('create/customer_saved_search')
  async customerSavedSearch(@Body() data: CustomerSavedSearchNotificationDto) {
    const result = await this.savedSearchesNotificationService.create(data);
    return result;
  }

  @Post('create/sales_saved_search')
  async salesSavedSearch(@Body() data: SalesSavedSearchNotificationDto) {
    const result = await this.savedSearchesForSalesNotificationService.create(data);
    return result;
  }
  
}
