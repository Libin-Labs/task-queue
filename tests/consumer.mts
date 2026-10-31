import { TaskQueue, type TaskQueueStats } from '@lp-labs/task-queue';

const queue = new TaskQueue({ concurrency: 2 });
const handle = queue.add(({ signal }) => (signal.aborted ? 'aborted' : 'ready'));
const stats: TaskQueueStats = queue.getStats();

void handle.promise;
void stats;
