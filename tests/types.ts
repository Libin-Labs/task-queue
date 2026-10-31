import { TaskCancelledError, TaskQueue } from '@lp-labs/task-queue';
import queueRequire = require('@lp-labs/task-queue');

const queue = new TaskQueue({ concurrency: 2, retries: 1, retryDelay: 10 });
const handle = queue.add(
  async ({ id, attempt, signal }) => {
    const taskId: string = id;
    const taskAttempt: number = attempt;
    const taskSignal: AbortSignal = signal;
    return `${taskId}-${taskAttempt}-${taskSignal.aborted}`;
  },
  { priority: 1, backoffFactor: 2 },
);

const taskResult: Promise<string> = handle.promise;
const cancellationInitiated: boolean = handle.cancel('stop');
const statsAreIdle: boolean = queue.stats.idle && queue.isIdle;
const cancellationReason: unknown = new TaskCancelledError('stop').reason;
const cjsConstructor: typeof queueRequire.TaskQueue = queueRequire.TaskQueue;

void taskResult;
void cancellationInitiated;
void statsAreIdle;
void cancellationReason;
void cjsConstructor;

// @ts-expect-error concurrency must be a positive safe integer at runtime, and its type is numeric
new TaskQueue({ concurrency: 'two' });
// @ts-expect-error task callbacks must be functions
queue.add(42);
