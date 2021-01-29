import { OnQueueActive, Process, Processor } from '@nestjs/bull';
import { Logger } from '@nestjs/common';
import { Job } from 'bull';
import { CustomerSavedSearchNotificationDto } from 'src/notification/dto/notification.dto';
import {
  SavedSearchesNotificationService,
} from '../../notification/notification.service';

@Processor('saved_searches')
export class SavedSearchProcessor {
  private readonly logger = new Logger(SavedSearchProcessor.name);

  constructor(
    private customerNotificationService: SavedSearchesNotificationService,
  ) { }

  @Process('saved_searches_queue')
  async handleTranscode(job: Job) {
    const jobs = job.data;
    for (let k of jobs) {
      await this.processSavedSearch(k);
    }
  }

  async processSavedSearch({ email = null,name=null, results = [], unique_vehicles=0 }) {
    if (!email || results.length === 0)
      return;

    try {
      const data = { email, customerName:name, results, uniqueVehicles: unique_vehicles };
      let resCustomer = await this.customerNotificationService.create(data as CustomerSavedSearchNotificationDto);
    }
    catch (error) {
      Logger.error('Failed to send notification: ' + error);
    }
  }


}
