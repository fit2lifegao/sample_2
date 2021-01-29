import { Injectable } from '@nestjs/common';
import { Queue } from 'bull';
import { Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { SavedVehicleSearchesService } from './saved-vehicle-searches.service';

@Injectable()
export class SavedSearchHandlerService {
    constructor(
        @InjectQueue('saved_searches') private savedSearchesQueue: Queue,
        private readonly savedVehicleSearchesService: SavedVehicleSearchesService,
        ) {}

    async run() {
        try{
            const results = await this.savedVehicleSearchesService.getAllResultsGroupedByEmail();
            return await this.savedSearchesQueue.add('saved_searches_queue', results);
        }
        catch(error) {
            Logger.error(error);
        }
     }
}
