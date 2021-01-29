import { IsEmail, IsNotEmpty, IsArray } from 'class-validator';

export class CreateNotificationDto {
    @IsNotEmpty()
    @IsArray()
    recipients: Array<string>;
    @IsNotEmpty()
    subject: string;
    @IsNotEmpty()
    topic: string;
    bodyHtml: string;
    bodyText: string;
    onClickUrl: string;
}

export class CheckoutNotificationDto {
    @IsNotEmpty()
    firstName: string;
    @IsNotEmpty()
    lastName: string;
    @IsNotEmpty()
    email: string;
    @IsNotEmpty()
    vehicleID: number;

    onClickUrl: string;
}

export class CreditAppNotificationDto {
    @IsNotEmpty()
    firstName: string;
    @IsNotEmpty()
    lastName: string;
    @IsNotEmpty()
    email: string;

    onClickUrl: string;
}

export class CustomerSavedSearchNotificationDto {
    @IsNotEmpty()
    email: string;
    customerName: string;
    additional_details: string;
    province: string;
    results: Array<object>;
    uniqueVehicles: number;
}

export class SalesSavedSearchNotificationDto {
    @IsNotEmpty()
    email: string;
    customerName: string;
    additional_details: string;
    province: string;
    search_filters: object;
}