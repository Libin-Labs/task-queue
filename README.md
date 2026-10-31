# @lp-labs/task-queue

Package version: `0.1.0`. License: MIT.

A small, zero-runtime-dependency TypeScript utility for running asynchronous tasks with bounded concurrency, stable priority ordering, cancellation, and retries.

## Installation

```sh
npm install @lp-labs/task-queue
```

The package provides ESM and CommonJS entry points with matching TypeScript declarations.

## Quick start

```ts
import { TaskQueue } from "@lp-labs/task-queue";

const queue = new TaskQueue({ concurrency: 3 });
const task = queue.add(async ({ signal, attempt }) => {
  const response = await fetch("https://example.com/data", { signal });
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return { attempt, data: await response.json() };
});

try {
  const result = await task.promise;
  console.log(result);
} finally {
  await queue.onIdle();
  queue.dispose();
}
```

## API

### `new TaskQueue(options?)`

All options are optional:

| Option | Default | Description |
| --- | --- | --- |
| `concurrency` | `1` | Maximum number of callbacks executing at once. Must be a positive safe integer. |
| `retries` | `0` | Number of retries after the first failed attempt. Must be a non-negative safe integer. |
| `retryDelay` | `0` | Delay in milliseconds before a retry, or `(attempt, error) => delayMs`. |
| `backoffFactor` | `2` | Multiplier applied to a numeric `retryDelay` after each failed attempt. Must be finite and non-negative. |

`retryDelay` callback results are used directly as the delay for that retry; they are not multiplied by `backoffFactor`. Delays must be finite values from `0` through `2,147,483,647` milliseconds. Retries apply to any callback rejection or synchronous throw; there is no built-in error filter.

### `queue.add(task, options?)`

Adds a task and returns a `TaskHandle<T>`:

```ts
const handle = queue.add(
  async ({ id, attempt, signal }) => {
    // Use signal for cooperative cancellation.
    return { id, attempt, aborted: signal.aborted };
  },
  { priority: 5, retries: 2 }
);

const result = await handle.promise;
```

The task receives a `TaskContext` containing:

- `id`: queue-local task identifier.
- `attempt`: one-based invocation count, including retries.
- `signal`: an `AbortSignal` aborted when cancellation is requested.

Per-task options can override the queue defaults: `priority` (finite number; higher values run first), `retries`, `retryDelay`, and `backoffFactor`. Provide an optional `signal` to cancel the task when another `AbortSignal` is aborted. Equal-priority queued tasks run FIFO. A retry re-enters at the tail of its priority group when its delay expires.

### `TaskHandle<T>`

- `id`: task identifier.
- `promise`: fulfills with the callback's value or rejects with its terminal error.
- `cancel(reason?)`: returns `true` only when it initiates cancellation; returns `false` after cancellation was already requested or the task settled.

Queued and backoff-waiting tasks are removed and their promises reject immediately with `TaskCancelledError`. A running task receives an abort signal, but cancellation cannot forcibly stop arbitrary JavaScript. Its queue slot remains occupied and its promise remains pending until its callback settles. If it then fulfills, the handle rejects with `TaskCancelledError`; if the callback rejects, that callback error is preserved instead. Code that needs prompt shutdown should observe the signal and settle promptly.

`TaskCancelledError.reason` contains the supplied cancellation reason.

### Scheduling and observation

- `pause()` prevents queued tasks from starting. Already-running callbacks continue.
- `resume()` resumes scheduling. Both methods are harmless after disposal.
- `clear({ cancelRunning?, reason? })` cancels all queued and retry-waiting tasks; running tasks are unaffected unless `cancelRunning` is `true`. It returns the number of tasks for which cancellation was initiated. A running task is counted when its signal is aborted, not when its callback settles.
- `stats` and `getStats()` return a snapshot with `queued`, `running`, `waitingRetries`, `completed`, `failed`, `cancelled`, `submitted`, `paused`, `disposed`, and `idle`.
- `isIdle` and `onIdle()` report/wait for there being no queued, running, or retry-waiting tasks. Waiting retries are deliberately not idle, even though they consume no execution slot.
- `subscribe(listener)` immediately calls the listener with a snapshot and then notifies it of changes. It returns an unsubscribe function. Listener exceptions are reported to `console.error` and do not interrupt queue processing.

### Disposal

`dispose({ cancelRunning?, reason? })` is idempotent, prevents new tasks, and cancels queued and retry-waiting tasks. It aborts running tasks by default. Pass `{ cancelRunning: false }` to allow callbacks already running to finish naturally. Disposal does not force-stop callbacks or wait for them; use `onIdle()` if you need to wait until they actually settle. A running callback that ignores its signal still holds its slot and keeps the queue non-idle until it settles.

Each task promise settles at most once; a running task that never settles can keep its handle pending after cancellation. The queue attaches an internal rejection handler to prevent ignored handles from producing unhandled rejections; callers can still observe the original rejection by awaiting `handle.promise`. Failed callback errors are not converted into successes. If retry-delay calculation itself throws or returns an invalid delay after a callback failure, the handle rejects with an `AggregateError` containing both errors.

## Exported types, errors, and boundaries

Runtime named exports are `TaskQueue` and `TaskCancelledError`; there is no
default export. Type-only exports are `TaskContext`, `TaskFunction<T>`,
`RetryDelay`, `TaskOptions`, `TaskQueueOptions`, `TaskHandle<T>`,
`TaskQueueStats`, `TaskQueueListener`, `ClearOptions`, and `DisposeOptions`.

- `TaskFunction<T> = (context: TaskContext) => T | PromiseLike<T>`.
- `RetryDelay = number | ((attempt: number, error: unknown) => number)`;
  attempt is the one-based failed attempt, not the next invocation.
- `TaskOptions` contains optional readonly `priority` (default `0`), `retries`,
  `retryDelay`, `backoffFactor`, and `signal`; retry defaults inherit the queue.
- `TaskQueueOptions` contains the four constructor options in the table above.
- `TaskQueueStats` fields are readonly: the seven counters (`queued`, `running`,
  `waitingRetries`, `completed`, `failed`, `cancelled`, `submitted`) use numbers; `paused`,
  `disposed`, and `idle` use booleans. `queued`, `running`, and `waitingRetries`
  are current counts; `completed`, `failed`, `cancelled`, and `submitted`
  accumulate for the queue lifetime.
- `TaskQueueListener = (stats: TaskQueueStats) => void`.
- `ClearOptions` and `DisposeOptions` each have optional readonly
  `cancelRunning?: boolean` and `reason?: unknown`. Clear defaults to false;
  dispose defaults to true.

`concurrency` is a readonly property on the queue. `pause()`, `resume()`, and
`dispose(options?)` return void; `clear(options?)` returns a count;
`onIdle()` returns `Promise<void>`. `TaskCancelledError(reason?: unknown)`
has name `"TaskCancelledError"`, message `"Task was cancelled"`, and readonly
`reason`. Invalid numeric options throw `RangeError`, invalid callbacks or
retry-delay types throw `TypeError`, and adding after disposal throws `Error`.

An ES2022 runtime with AbortController, promises, timers, and console is needed;
`fetch` is only needed by the first example. TypeScript consumers need DOM
declarations for AbortSignal, not Node ambient types. No browser globals are
accessed on module import, no workers are created, and no React/CSS is required.
Concurrency overlaps asynchronous work, not CPU threads. Queue size and retained
tasks are unbounded; there is no persistence, rate limit, task timeout, worker
offloading, or starvation prevention for continually arriving high priorities.
The utility has no UI or accessibility behavior; application UI should announce
progress and cancellation.

## License

[MIT](./LICENSE).

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
