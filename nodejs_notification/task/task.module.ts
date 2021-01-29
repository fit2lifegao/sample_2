import { Module } from '@nestjs/common';
import { TaskService } from './task.service';
import { SavedSearchHandlerModule } from '../customer/saved-vehicle-searches/saved-search-handler.module';

@Module({
  providers: [TaskService],
  imports: [SavedSearchHandlerModule]
})
export class TasksModule {}