import taskQueue = require('@lp-labs/task-queue');

const queue = new taskQueue.TaskQueue({ retries: 1 });
const handle = queue.add(({ attempt }) => attempt);
const stats: taskQueue.TaskQueueStats = queue.stats;

void handle.promise;
void stats;
