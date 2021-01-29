import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { ENV_PARAMS } from "../environment";
import { SavedSearchHandlerService } from "../customer/saved-vehicle-searches/saved-search-handler.service";
const INTERVAL = '0 16 * * 1-7'; //10 pm(sk time) everyday


@Injectable()
export class TaskService {
    private readonly logger = new Logger(TaskService.name);
    
    constructor(
        private savedSearchHandlerService: SavedSearchHandlerService
    ){}

    @Cron(INTERVAL)
    handleSavedSearches() {
        this.logger.debug('Called when the current second is 30');
        this.savedSearchHandlerService.run();
    }
}
