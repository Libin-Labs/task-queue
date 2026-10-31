import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';
import { TaskCancelledError, TaskQueue } from '../dist/esm/index.js';

const require = createRequire(import.meta.url);
const cjs = require('../dist/cjs/index.js');

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function eventually(predicate, message = 'condition was not met') {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await delay(2);
  }
  assert.fail(message);
}

test('ESM and CommonJS exports expose matching APIs', () => {
  assert.deepEqual(Object.keys(cjs).sort(), Object.keys(awaitlessImportKeys()).sort());
  assert.equal(typeof cjs.TaskQueue, 'function');
  assert.equal(typeof cjs.TaskCancelledError, 'function');
});

function awaitlessImportKeys() {
  return { TaskQueue, TaskCancelledError };
}

test('rejects invalid numeric configuration and per-task options', () => {
  for (const concurrency of [0, -1, Infinity, NaN, 1.5]) {
    assert.throws(() => new TaskQueue({ concurrency }), RangeError);
  }
  for (const retries of [-1, Infinity, NaN, 1.2]) {
    assert.throws(() => new TaskQueue({ retries }), RangeError);
  }
  assert.throws(() => new TaskQueue({ retryDelay: Infinity }), RangeError);
  assert.throws(() => new TaskQueue({ backoffFactor: NaN }), RangeError);
  const queue = new TaskQueue();
  assert.throws(() => queue.add(() => 1, { priority: Infinity }), RangeError);
  assert.throws(() => queue.add(() => 1, { retries: -1 }), RangeError);
  assert.throws(() => queue.add(() => 1, { retryDelay: NaN }), RangeError);
  assert.throws(() => queue.add(() => 1, { backoffFactor: Infinity }), RangeError);
});

test('settles sync values, async values, and synchronous throws', async () => {
  const queue = new TaskQueue({ concurrency: 2 });
  const error = new Error('sync failure');
  const success = queue.add(() => 7);
  const syncFailure = queue.add(() => {
    throw error;
  });
  assert.equal(await success.promise, 7);
  await assert.rejects(syncFailure.promise, (received) => received === error);
  assert.deepEqual(
    { completed: queue.stats.completed, failed: queue.stats.failed, running: queue.stats.running },
    { completed: 1, failed: 1, running: 0 },
  );
  await queue.onIdle();
});

test('enforces the concurrency bound until callbacks actually settle after cancellation', async () => {
  const queue = new TaskQueue({ concurrency: 1 });
  const gate = deferred();
  const started = deferred();
  let active = 0;
  let maximum = 0;
  const first = queue.add(async ({ signal }) => {
    active += 1;
    maximum = Math.max(maximum, active);
    started.resolve();
    await gate.promise;
    active -= 1;
    return signal.aborted;
  });
  const secondStarted = deferred();
  const second = queue.add(() => {
    active += 1;
    maximum = Math.max(maximum, active);
    active -= 1;
    secondStarted.resolve();
  });
  await started.promise;
  assert.equal(first.cancel('stop'), true);
  await delay(5);
  assert.equal(queue.stats.running, 1);
  assert.equal(queue.stats.queued, 1);
  assert.equal(maximum, 1);
  gate.resolve();
  await assert.rejects(first.promise, TaskCancelledError);
  await secondStarted.promise;
  await second.promise;
  assert.equal(maximum, 1);
  assert.equal(queue.stats.cancelled, 1);
});

test('orders by descending priority and preserves FIFO among equal priorities', async () => {
  const queue = new TaskQueue({ concurrency: 1 });
  queue.pause();
  const order = [];
  queue.add(() => order.push('first'));
  queue.add(() => order.push('priority-low'), { priority: -1 });
  queue.add(() => order.push('priority-high-a'), { priority: 3 });
  queue.add(() => order.push('priority-high-b'), { priority: 3 });
  queue.resume();
  await queue.onIdle();
  assert.deepEqual(order, ['priority-high-a', 'priority-high-b', 'first', 'priority-low']);
});

test('pause blocks queued work, while resume restarts scheduling', async () => {
  const queue = new TaskQueue({ concurrency: 1 });
  queue.pause();
  const first = queue.add(() => 'one');
  const second = queue.add(() => 'two');
  await delay(5);
  assert.equal(queue.stats.queued, 2);
  assert.equal(queue.stats.running, 0);
  queue.resume();
  assert.deepEqual(await Promise.all([first.promise, second.promise]), ['one', 'two']);
});

test('cancels queued tasks immediately and observes external AbortSignal', async () => {
  const queue = new TaskQueue({ concurrency: 1 });
  const gate = deferred();
  const running = queue.add(() => gate.promise);
  const queued = queue.add(() => assert.fail('cancelled queued task ran'));
  const controller = new AbortController();
  const externallyCancelled = queue.add(() => assert.fail('aborted task ran'), {
    signal: controller.signal,
  });
  assert.equal(queued.cancel('queued'), true);
  controller.abort('external');
  await assert.rejects(
    queued.promise,
    (error) => error instanceof TaskCancelledError && error.reason === 'queued',
  );
  await assert.rejects(
    externallyCancelled.promise,
    (error) => error instanceof TaskCancelledError && error.reason === 'external',
  );
  assert.equal(queue.stats.cancelled, 2);
  gate.resolve('done');
  await running.promise;
});

test('running abort is cooperative and waits for callback settlement', async () => {
  const queue = new TaskQueue({ concurrency: 1 });
  const gate = deferred();
  const started = deferred();
  let receivedSignal;
  const handle = queue.add(async ({ signal }) => {
    receivedSignal = signal;
    started.resolve();
    await gate.promise;
    return 'settled after abort';
  });
  await started.promise;
  assert.equal(handle.cancel('requested'), true);
  assert.equal(receivedSignal.aborted, true);
  assert.equal(queue.stats.running, 1);
  gate.resolve();
  await assert.rejects(handle.promise, TaskCancelledError);
  assert.equal(queue.stats.running, 0);
});

test('preserves callback rejection after running cancellation', async () => {
  const queue = new TaskQueue();
  const gate = deferred();
  const error = new Error('callback failure');
  const handle = queue.add(async () => {
    await gate.promise;
    throw error;
  });
  assert.equal(handle.cancel('stop'), true);
  gate.resolve();
  await assert.rejects(handle.promise, (received) => received === error);
  assert.equal(queue.stats.failed, 1);
  assert.equal(queue.stats.cancelled, 0);
});

test('retries with exponential backoff and exposes attempts to callbacks', async () => {
  const queue = new TaskQueue({ retries: 2, retryDelay: 5, backoffFactor: 2 });
  const attempts = [];
  const handle = queue.add(({ attempt }) => {
    attempts.push(attempt);
    if (attempt < 3) throw new Error(`attempt ${attempt}`);
    return 'ok';
  });
  assert.equal(await handle.promise, 'ok');
  assert.deepEqual(attempts, [1, 2, 3]);
  assert.equal(queue.stats.completed, 1);
});

test('waiting retries consume no slot and keep the queue non-idle', async () => {
  const queue = new TaskQueue({ concurrency: 1, retries: 1, retryDelay: 30 });
  const secondStarted = deferred();
  let attempts = 0;
  const retried = queue.add(() => {
    attempts += 1;
    if (attempts === 1) throw new Error('retry me');
    return 'retried';
  });
  await eventually(() => queue.stats.waitingRetries === 1);
  assert.equal(queue.stats.idle, false);
  const idle = queue.onIdle();
  const second = queue.add(() => {
    secondStarted.resolve();
    return 'second';
  });
  await secondStarted.promise;
  assert.equal(await second.promise, 'second');
  assert.equal(queue.stats.waitingRetries, 1);
  assert.equal(queue.stats.idle, false);
  assert.equal(await retried.promise, 'retried');
  await idle;
  assert.equal(queue.isIdle, true);
});

test('cancels tasks while waiting for retry backoff', async () => {
  const queue = new TaskQueue({ retries: 4, retryDelay: 100 });
  const handle = queue.add(() => {
    throw new Error('again');
  });
  await eventually(() => queue.stats.waitingRetries === 1);
  assert.equal(handle.cancel('no retry'), true);
  await assert.rejects(
    handle.promise,
    (error) => error instanceof TaskCancelledError && error.reason === 'no retry',
  );
  assert.equal(queue.stats.waitingRetries, 0);
  assert.equal(queue.stats.cancelled, 1);
});

test('uses a retry delay callback and preserves terminal callback errors', async () => {
  const delays = [];
  const queue = new TaskQueue({
    retries: 1,
    retryDelay: (attempt, error) => {
      delays.push([attempt, error.message]);
      return 0;
    },
  });
  const taskError = new Error('terminal task error');
  const handle = queue.add(() => {
    throw taskError;
  });
  await assert.rejects(handle.promise, (error) => error === taskError);
  assert.deepEqual(delays, [[1, 'terminal task error']]);
  assert.equal(queue.stats.failed, 1);

  const callbackError = new Error('delay callback error');
  const brokenQueue = new TaskQueue({
    retries: 1,
    retryDelay: () => {
      throw callbackError;
    },
  });
  const broken = brokenQueue.add(() => {
    throw taskError;
  });
  await assert.rejects(broken.promise, (error) => {
    assert.ok(error instanceof AggregateError);
    assert.deepEqual(error.errors, [taskError, callbackError]);
    return true;
  });
});

test('clear cancels queued and retrying tasks; dispose aborts running tasks and rejects future adds', async () => {
  const queue = new TaskQueue({ concurrency: 2, retries: 2, retryDelay: 100 });
  const gate = deferred();
  const started = deferred();
  const running = queue.add(async () => {
    started.resolve();
    await gate.promise;
  });
  await started.promise;
  const retrying = queue.add(() => {
    throw new Error('retry later');
  });
  await eventually(() => queue.stats.waitingRetries === 1);
  queue.pause();
  const queued = queue.add(() => 'never');
  assert.equal(queue.clear(), 2);
  await assert.rejects(queued.promise, TaskCancelledError);
  await assert.rejects(retrying.promise, TaskCancelledError);
  assert.equal(queue.stats.running, 1);
  queue.dispose();
  assert.equal(queue.stats.disposed, true);
  assert.equal(queue.stats.running, 1);
  assert.throws(() => queue.add(() => 1), /disposed/);
  gate.resolve();
  await assert.rejects(running.promise, TaskCancelledError);
  assert.equal(queue.stats.running, 0);
});

test('dispose can leave running callbacks alone when requested and is idempotent', async () => {
  const queue = new TaskQueue();
  const gate = deferred();
  const handle = queue.add(() => gate.promise);
  queue.dispose({ cancelRunning: false });
  queue.dispose();
  assert.equal(queue.stats.running, 1);
  gate.resolve('completed');
  assert.equal(await handle.promise, 'completed');
  assert.equal(queue.stats.completed, 1);
  assert.throws(() => queue.add(() => 1), /disposed/);
});

test('stats notifications and idle promises reflect active work', async () => {
  const queue = new TaskQueue();
  const gate = deferred();
  const snapshots = [];
  const unsubscribe = queue.subscribe((stats) => snapshots.push(stats));
  const handle = queue.add(() => gate.promise);
  const idle = queue.onIdle();
  await eventually(() => snapshots.some((stats) => stats.running === 1));
  let idleResolved = false;
  void idle.then(() => {
    idleResolved = true;
  });
  await delay(5);
  assert.equal(idleResolved, false);
  gate.resolve('done');
  await handle.promise;
  await idle;
  assert.equal(queue.stats.idle, true);
  assert.ok(snapshots.some((stats) => stats.completed === 1));
  unsubscribe();
});
