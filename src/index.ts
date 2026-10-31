export interface TaskContext {
  readonly id: string;
  readonly attempt: number;
  readonly signal: AbortSignal;
}

export type TaskFunction<T> = (context: TaskContext) => T | PromiseLike<T>;
export type RetryDelay = number | ((attempt: number, error: unknown) => number);

export interface TaskOptions {
  readonly priority?: number;
  readonly retries?: number;
  readonly retryDelay?: RetryDelay;
  readonly backoffFactor?: number;
  readonly signal?: AbortSignal;
}

export interface TaskQueueOptions {
  readonly concurrency?: number;
  readonly retries?: number;
  readonly retryDelay?: RetryDelay;
  readonly backoffFactor?: number;
}

export interface TaskHandle<T> {
  readonly id: string;
  readonly promise: Promise<T>;
  cancel(reason?: unknown): boolean;
}

export interface TaskQueueStats {
  readonly queued: number;
  readonly running: number;
  readonly waitingRetries: number;
  readonly completed: number;
  readonly failed: number;
  readonly cancelled: number;
  readonly submitted: number;
  readonly paused: boolean;
  readonly disposed: boolean;
  readonly idle: boolean;
}

export type TaskQueueListener = (stats: TaskQueueStats) => void;

export interface ClearOptions {
  readonly cancelRunning?: boolean;
  readonly reason?: unknown;
}

export interface DisposeOptions {
  readonly cancelRunning?: boolean;
  readonly reason?: unknown;
}

type EntryState = 'queued' | 'running' | 'waiting-retry' | 'settled';

interface QueueEntry<T = unknown> {
  readonly id: string;
  readonly task: TaskFunction<T>;
  readonly priority: number;
  readonly retries: number;
  readonly retryDelay: RetryDelay;
  readonly backoffFactor: number;
  readonly controller: AbortController;
  readonly resolve: (value: T | PromiseLike<T>) => void;
  readonly reject: (reason?: unknown) => void;
  readonly promise: Promise<T>;
  readonly externalSignal?: AbortSignal;
  externalAbortListener?: () => void;
  state: EntryState;
  attempt: number;
  sequence: number;
  retryTimer?: ReturnType<typeof globalThis.setTimeout>;
  cancelRequested: boolean;
  cancelReason?: unknown;
}

const MAX_TIMEOUT = 2_147_483_647;

export class TaskCancelledError extends Error {
  readonly reason: unknown;

  constructor(reason?: unknown) {
    super('Task was cancelled');
    this.name = 'TaskCancelledError';
    this.reason = reason;
  }
}

function assertNonNegativeInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

function assertPositiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
}

function validateDelay(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0 || value > MAX_TIMEOUT) {
    throw new RangeError(`${name} must be finite and between 0 and ${MAX_TIMEOUT}`);
  }
}

function validateRetryDelay(value: RetryDelay, name: string): void {
  if (typeof value === 'number') {
    validateDelay(value, name);
  } else if (typeof value !== 'function') {
    throw new TypeError(`${name} must be a number or a function`);
  }
}

function validateBackoffFactor(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be a finite non-negative number`);
  }
}

export class TaskQueue {
  readonly concurrency: number;

  private readonly defaultRetries: number;
  private readonly defaultRetryDelay: RetryDelay;
  private readonly defaultBackoffFactor: number;
  private readonly queued: QueueEntry<any>[] = [];
  private readonly active = new Set<QueueEntry<any>>();
  private readonly listeners = new Set<TaskQueueListener>();
  private readonly idleWaiters = new Set<() => void>();
  private sequence = 0;
  private idSequence = 0;
  private completed = 0;
  private failed = 0;
  private cancelled = 0;
  private paused = false;
  private disposed = false;

  constructor(options: TaskQueueOptions = {}) {
    this.concurrency = options.concurrency ?? 1;
    this.defaultRetries = options.retries ?? 0;
    this.defaultRetryDelay = options.retryDelay ?? 0;
    this.defaultBackoffFactor = options.backoffFactor ?? 2;

    assertPositiveInteger('concurrency', this.concurrency);
    assertNonNegativeInteger('retries', this.defaultRetries);
    validateRetryDelay(this.defaultRetryDelay, 'retryDelay');
    validateBackoffFactor(this.defaultBackoffFactor, 'backoffFactor');
  }

  get stats(): TaskQueueStats {
    return this.getStats();
  }

  get isIdle(): boolean {
    return this.active.size === 0;
  }

  getStats(): TaskQueueStats {
    let queued = 0;
    let running = 0;
    let waitingRetries = 0;

    for (const entry of this.active) {
      if (entry.state === 'queued') queued += 1;
      else if (entry.state === 'running') running += 1;
      else if (entry.state === 'waiting-retry') waitingRetries += 1;
    }

    return Object.freeze({
      queued,
      running,
      waitingRetries,
      completed: this.completed,
      failed: this.failed,
      cancelled: this.cancelled,
      submitted: this.idSequence,
      paused: this.paused,
      disposed: this.disposed,
      idle: this.active.size === 0,
    });
  }

  subscribe(listener: TaskQueueListener): () => void {
    if (typeof listener !== 'function') {
      throw new TypeError('listener must be a function');
    }
    this.listeners.add(listener);
    this.notifyListener(listener, this.getStats());
    return () => this.listeners.delete(listener);
  }

  onIdle(): Promise<void> {
    if (this.isIdle) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.add(resolve));
  }

  add<T>(task: TaskFunction<T>, options: TaskOptions = {}): TaskHandle<T> {
    if (this.disposed) throw new Error('Cannot add tasks to a disposed queue');
    if (typeof task !== 'function') throw new TypeError('task must be a function');

    const priority = options.priority ?? 0;
    const retries = options.retries ?? this.defaultRetries;
    const retryDelay = options.retryDelay ?? this.defaultRetryDelay;
    const backoffFactor = options.backoffFactor ?? this.defaultBackoffFactor;

    if (!Number.isFinite(priority)) {
      throw new RangeError('priority must be finite');
    }
    assertNonNegativeInteger('retries', retries);
    validateRetryDelay(retryDelay, 'retryDelay');
    validateBackoffFactor(backoffFactor, 'backoffFactor');

    let resolve!: (value: T | PromiseLike<T>) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    promise.catch(() => undefined);

    const entry: QueueEntry<T> = {
      id: `task-${++this.idSequence}`,
      task,
      priority,
      retries,
      retryDelay,
      backoffFactor,
      controller: new AbortController(),
      resolve,
      reject,
      promise,
      externalSignal: options.signal,
      state: 'queued',
      attempt: 0,
      sequence: this.sequence++,
      cancelRequested: false,
    };

    this.active.add(entry);
    if (entry.externalSignal) {
      entry.externalAbortListener = () => {
        this.cancelEntry(entry, entry.externalSignal?.reason);
      };
      if (entry.externalSignal.aborted) {
        entry.externalAbortListener();
      } else {
        entry.externalSignal.addEventListener('abort', entry.externalAbortListener, { once: true });
      }
    }

    if (entry.state === 'queued') {
      this.insertQueued(entry);
      this.publish();
      this.pump();
    }

    return {
      id: entry.id,
      promise,
      cancel: (reason?: unknown) => this.cancelEntry(entry, reason),
    };
  }

  pause(): void {
    if (this.disposed) return;
    if (this.paused) return;
    this.paused = true;
    this.publish();
  }

  resume(): void {
    if (this.disposed || !this.paused) return;
    this.paused = false;
    this.publish();
    this.pump();
  }

  clear(options: ClearOptions = {}): number {
    const reason = options.reason;
    let count = 0;
    for (const entry of [...this.active]) {
      if (entry.state === 'running' && options.cancelRunning) {
        if (this.cancelEntry(entry, reason)) count += 1;
      } else if (entry.state !== 'running') {
        if (this.cancelEntry(entry, reason)) count += 1;
      }
    }
    return count;
  }

  dispose(options: DisposeOptions = {}): void {
    if (this.disposed) return;
    this.disposed = true;
    this.paused = true;
    this.clear({
      cancelRunning: options.cancelRunning ?? true,
      reason: options.reason,
    });
    this.publish();
  }

  private insertQueued(entry: QueueEntry<any>): void {
    let index = 0;
    while (
      index < this.queued.length &&
      (this.queued[index].priority > entry.priority ||
        (this.queued[index].priority === entry.priority &&
          this.queued[index].sequence < entry.sequence))
    ) {
      index += 1;
    }
    this.queued.splice(index, 0, entry);
  }

  private pump(): void {
    if (this.paused || this.disposed) return;
    while (this.queued.length > 0 && this.runningCount() < this.concurrency) {
      const entry = this.queued.shift();
      if (!entry || entry.state !== 'queued') continue;
      entry.state = 'running';
      entry.attempt += 1;
      this.publish();
      void this.execute(entry);
    }
  }

  private async execute<T>(entry: QueueEntry<T>): Promise<void> {
    let value: T | undefined;
    let taskError: unknown;
    let succeeded = false;

    try {
      value = await entry.task({
        id: entry.id,
        attempt: entry.attempt,
        signal: entry.controller.signal,
      });
      succeeded = true;
    } catch (error) {
      taskError = error;
    }

    if (entry.state !== 'running') return;
    if (succeeded) {
      if (entry.cancelRequested) {
        this.settleCancelled(entry, entry.cancelReason);
      } else {
        this.settleCompleted(entry, value as T);
      }
      this.pump();
      return;
    }

    if (!entry.cancelRequested && !this.disposed && entry.attempt <= entry.retries) {
      let delay: number;
      try {
        delay = this.getRetryDelay(entry, taskError);
        validateDelay(delay, 'computed retry delay');
      } catch (delayError) {
        this.settleFailed(
          entry,
          new AggregateError(
            [taskError, delayError],
            'Task failed and retry delay calculation also failed',
          ),
        );
        this.pump();
        return;
      }
      this.waitToRetry(entry, delay);
    } else {
      this.settleFailed(entry, taskError);
      this.pump();
    }
  }

  private getRetryDelay(entry: QueueEntry<any>, error: unknown): number {
    if (typeof entry.retryDelay === 'function') {
      return entry.retryDelay(entry.attempt, error);
    }
    return entry.retryDelay * entry.backoffFactor ** (entry.attempt - 1);
  }

  private waitToRetry(entry: QueueEntry<any>, delay: number): void {
    entry.state = 'waiting-retry';
    entry.retryTimer = globalThis.setTimeout(() => {
      entry.retryTimer = undefined;
      if (entry.state !== 'waiting-retry') return;
      if (this.disposed || entry.cancelRequested) {
        this.settleCancelled(entry, entry.cancelReason);
        return;
      }
      entry.sequence = this.sequence++;
      entry.state = 'queued';
      this.insertQueued(entry);
      this.publish();
      this.pump();
    }, delay);
    this.publish();
    this.pump();
  }

  private runningCount(): number {
    let count = 0;
    for (const entry of this.active) {
      if (entry.state === 'running') count += 1;
    }
    return count;
  }

  private cancelEntry(entry: QueueEntry<any>, reason?: unknown): boolean {
    if (entry.state === 'settled' || entry.cancelRequested) return false;
    entry.cancelRequested = true;
    entry.cancelReason = reason;

    try {
      entry.controller.abort(reason);
    } catch {
      entry.controller.abort();
    }

    if (entry.state === 'running') {
      this.publish();
      return true;
    }

    if (entry.state === 'queued') {
      const index = this.queued.indexOf(entry);
      if (index >= 0) this.queued.splice(index, 1);
    } else if (entry.state === 'waiting-retry' && entry.retryTimer !== undefined) {
      globalThis.clearTimeout(entry.retryTimer);
      entry.retryTimer = undefined;
    }
    this.settleCancelled(entry, reason);
    this.pump();
    return true;
  }

  private settleCompleted<T>(entry: QueueEntry<T>, value: T): void {
    this.finish(entry);
    this.completed += 1;
    entry.resolve(value);
    this.publish();
  }

  private settleFailed(entry: QueueEntry<any>, error: unknown): void {
    this.finish(entry);
    this.failed += 1;
    entry.reject(error);
    this.publish();
  }

  private settleCancelled(entry: QueueEntry<any>, reason?: unknown): void {
    this.finish(entry);
    this.cancelled += 1;
    entry.reject(new TaskCancelledError(reason));
    this.publish();
  }

  private finish(entry: QueueEntry<any>): void {
    entry.state = 'settled';
    if (entry.retryTimer !== undefined) {
      globalThis.clearTimeout(entry.retryTimer);
      entry.retryTimer = undefined;
    }
    if (entry.externalSignal && entry.externalAbortListener) {
      entry.externalSignal.removeEventListener('abort', entry.externalAbortListener);
      entry.externalAbortListener = undefined;
    }
    this.active.delete(entry);
  }

  private publish(): void {
    const stats = this.getStats();
    if (stats.idle) {
      for (const resolve of this.idleWaiters) resolve();
      this.idleWaiters.clear();
    }
    for (const listener of [...this.listeners]) {
      this.notifyListener(listener, stats);
    }
  }

  private notifyListener(listener: TaskQueueListener, stats: TaskQueueStats): void {
    try {
      listener(stats);
    } catch (error) {
      globalThis.console?.error('TaskQueue listener threw an error:', error);
    }
  }
}
