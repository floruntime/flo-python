# flo-python

Python SDK for the [Flo](https://github.com/floruntime) distributed systems platform.

## Installation

```bash
pip install flo-python
```

## Quick Start

```python
import asyncio
from flo import FloClient

async def main():
    # Connect to Flo server
    async with FloClient("localhost:9000") as client:
        # KV operations
        await client.kv.put("user:123", b"John Doe")
        value = await client.kv.get("user:123")
        print(f"Got: {value.decode()}")

        # Queue operations
        seq = await client.queue.enqueue("tasks", b'{"task": "process"}')
        print(f"Enqueued message with seq: {seq}")

asyncio.run(main())
```

## Features

- **KV Store**: Versioned key-value storage with MVCC, TTL, and optimistic locking
- **Queues**: Priority task queues, at-least-once: a dequeue leases its messages until they're completed
- **Streams**: Append-only logs with consumer groups for distributed processing
- **Actions**: Registered tasks with configurable timeouts and retries
- **Workers**: Distributed task execution with lease management and heartbeats
- **Async/await**: Native asyncio support for high-performance applications
- **Type hints**: Full type annotations for IDE support

## KV Operations

### Get

```python
from flo import GetOptions, WaitFor

# Simple get
value = await client.kv.get("key")
if value is not None:
    print(value.decode())

# Wait up to 2 seconds for the key to exist (answers at once if it does)
# (wait_ms is at most 300000; the client waits timeout_ms on top of it;
# 0 means don't wait)
value = await client.kv.get("key", GetOptions(wait_ms=2000))

# Wait up to 2 seconds for the key's next version
value = await client.kv.get("key", GetOptions(wait_ms=2000, wait_for=WaitFor.CHANGE))

# Get with namespace override
value = await client.kv.get("key", GetOptions(namespace="other"))
```

### Put

```python
from flo import Condition, PutOptions

# Simple put
await client.kv.put("key", b"value")

# Put with TTL (expires in 1 hour)
await client.kv.put("session:abc", b"data", PutOptions(ttl_ms=3_600_000))

# Put with CAS (optimistic locking)
await client.kv.put("counter", b"2", PutOptions(cas_version=1))

# Put only if key doesn't exist
await client.kv.put("key", b"value", PutOptions(condition=Condition.IF_ABSENT))

# Put only if key exists
await client.kv.put("key", b"value", PutOptions(condition=Condition.IF_PRESENT))
```

### Delete

```python
from flo import DeleteOptions

# Returns whether the key existed; deleting a missing key is not an error
existed = await client.kv.delete("key")

# Delete only at a known version
await client.kv.delete("lock:resource", DeleteOptions(cas_version=held_version))
```

### Scan

```python
from flo import ScanOptions

# Names of keys with a prefix ("" for all), a page at a time (limit 1..1024,
# default 1024); the cursor is None on the last page
opts = ScanOptions(limit=100)
while True:
    result = await client.kv.scan("user:", opts)
    for name in result.names:
        print(name)
    if result.cursor is None:
        break
    opts = ScanOptions(cursor=result.cursor, limit=100)
```

### MGet

```python
# One entry per key, in order (at most 256 keys of at most 4096 bytes each).
# The keys may live on any shards: the server gathers them.
for e in await client.kv.mget(["user:1", "user:2"]):
    if e.found:
        print(e.key, e.version, e.value)
```

A key routes by its `{tag}`, the part between its first `{` and the next
`}`, or by the whole key when it has none. Keys that share a `{tag}`, such as
`{user:1}:name` and `{user:1}:email`, are stored on the same shard; that is
what a transaction needs.

### History

```python
from flo import HistoryOptions

# A page of versions newest first (limit 1..65, default 10), each with its write
# time in ms and whether it was a delete; empty for a missing key. `more` says
# older kept versions follow: ask again from before the last one.
opts = HistoryOptions(limit=10)
while True:
    page = await client.kv.history("user:123", opts)
    for entry in page.versions:
        print(f"v{entry.version} at {entry.at_ms}: {entry.value.decode()}")
    if not page.more or not page.versions:
        break
    opts = HistoryOptions(limit=10, before_version=page.versions[-1].version)
```

A key keeps at most 64 older versions, and 64 KiB of them; the oldest go first.

### Counters, TTL and exists

```python
from flo import KVExpireOptions, KVIncrOptions

res = await client.kv.incr("visits")  # +1
res = await client.kv.incr("visits", KVIncrOptions(delta=-5))
print(res.value, res.version)

# Set a key's TTL in ms; 0 clears it. cas_version makes lease renewal race-free.
await client.kv.expire("session:abc", 60_000)
await client.kv.expire("lease", 30_000, KVExpireOptions(cas_version=lease_version))
await client.kv.expire("session:abc", 0)

res = await client.kv.exists("session:abc")
if res:  # truthy when the key exists; res.exists says the same
    print(res.version)
```

### JSON documents

```python
# Path defaults to "$" (the whole document)
await client.kv.json_set("user:1", "$", b'{"name": "Ada", "tags": []}')
doc = await client.kv.json_get("user:1", "$.name")  # GetJsonResult or None
await client.kv.json_del("user:1", "$.tags")  # returns the new version
await client.kv.json_del("user:1")  # "$" deletes the key: returns None
```

### Transactions

```python
# Pinned to the {tag} of the key it begins with: every key in it shares it
async with await client.kv.begin("{user:123}") as txn:
    await txn.put("{user:123}:name", b"Jane")
    await txn.incr("{user:123}:visits")
    await txn.expire("{user:123}:session", 60_000)
    result = await txn.commit()
    print(result.commit_index, result.op_count)
```

A transaction runs on the shard that owns the key it begins with, pinned to
that key's `{tag}`, or to the whole key when it has none. A key in it that
routes elsewhere is refused with reason `KEYS_NOT_COLOCATED` (a
`BadRequestError` that didn't run, never retried): give every key in a
transaction the same `{tag}`.

A transaction holds at most 64 ops and 256 KiB of keys and values by default;
both are operator settings (`[kv] max_txn_ops`, `[kv] max_txn_bytes`). A
`condition`, `cas_version` or `incr` inside it is decided at `commit()`, not
when the op is buffered: one that fails refuses the commit, and nothing in the
transaction applies. Outside a transaction a failed `cas_version` or
`condition` raises the same conflict as always, though it is decided as the
write applies, so it still costs a commit.

## Queue Operations

### Enqueue

```python
from flo import EnqueueOptions

# Simple enqueue (a message holds at most 64 KiB)
seq = await client.queue.enqueue("tasks", b'{"task": "process"}')

# With priority (0-255, lower is taken first; default 0), ready in 5 s
seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10, delay_ms=5000))
```

A queue holds at most its cap of messages and bytes (ready, leased and dead
together), and a shard's queues their cap of bytes; both are server settings.
Past either, an enqueue raises `OverloadedError` (ran `NO`, its message naming
the queue and its caps). The client retries it like any overloaded refusal
that didn't run, for about 350 ms, before raising it: consume or purge the
queue first.

### Dequeue, Complete, Fail, Touch

Delivery is at least once. A dequeue leases its messages to its consumer
(`DequeueOptions(consumer=..., lease_ms=...)`, 30 s by default) and doesn't
consume them: complete each message within its lease, or it is ready again and
delivered once more, with `deliveries` one higher. A message failed or expired
five times is dead (see the DLQ below). `ack=True` completes the messages as
they're taken, a one-shot consume that comes back only if the leader fails in
between.

```python
from flo import DequeueOptions, QueueFailOptions

# Up to 10 messages, waiting up to 30 s for one
result = await client.queue.dequeue("tasks", 10, DequeueOptions(block_ms=30000))
done, failed = [], []
for msg in result.messages:
    try:
        process(msg.payload)
        done.append(msg)
    except Exception:
        failed.append(msg)

# Reports name the dequeue (pass its result) and the messages, all of them
# when none are given; the same consumer must send them.
if done:
    report = await client.queue.complete("tasks", result, done)
    # A message the report didn't apply to is in report.refused; for a
    # complete, GONE means it was done already, so report.lost has the rest.
    for r in report.lost:
        print(f"message {r.seq} went to another delivery: {r.reason}")
if failed:
    await client.queue.fail("tasks", result, failed, QueueFailOptions(delay_ms=1000))

# Keep a long job: with half its lease or less left, a touch runs the lease a
# full lease from now, up to 12 h from the dequeue
touched = await client.queue.touch("tasks", result)

# One-shot consume
result = await client.queue.dequeue("tasks", 10, DequeueOptions(ack=True))
```

### Dead Letters and Purge

`queue.purge` removes every message of a queue, ready, leased and dead, and
returns how many.

`queue.dlq_list` reads a page of dead letters, oldest first (`limit` 1..100):

```python
from flo import DlqListOptions

page = await client.queue.dlq_list("tasks", DlqListOptions(limit=100))
print(f"{page.total} dead letters")
while page.has_more:
    page = await client.queue.dlq_list("tasks", DlqListOptions(after=page.last_seq))
```

### Peek

```python
# The next ready messages (1..100, default 20), in dequeue order, without
# taking them; each shows up to 4 KiB of its payload (msg.size is its length)
result = await client.queue.peek("tasks", 5)
for msg in result.messages:
    print(f"Message {msg.seq}: {msg.payload}")
print("more follow" if result.more else "that's all")
```

## Stream Operations

Streams are append-only logs ideal for event sourcing, activity feeds, and real-time data pipelines.

### Append

```python
from flo import StreamAppendOptions

# Simple append
result = await client.stream.append("events", b'{"event": "click", "user": "123"}')
print(f"Appended at offset {result.offset}, timestamp {result.timestamp}")

# Append with partition key (for ordered processing within partition)
result = await client.stream.append(
    "events", payload,
    StreamAppendOptions(partition_key="user-123")
)

# Append with client-provided timestamp
result = await client.stream.append(
    "events", payload,
    StreamAppendOptions(timestamp=1699999999000)
)
```

### Read

```python
from flo import StreamID, StreamReadOptions

# Read from the beginning (default)
result = await client.stream.read("events")
for record in result.records:
    print(f"{record.id.timestamp_ms}-{record.id.sequence}: {record.payload}")

# Read the records after one already seen: start is exclusive
last = result.records[-1].id
result = await client.stream.read("events", StreamReadOptions(start=last, count=10))

# Read from the tail (latest records)
result = await client.stream.read("events", StreamReadOptions(tail=True, count=10))

# Blocking read (long polling): wait up to 30 s for records after `last`
result = await client.stream.read("events", StreamReadOptions(start=last, block_ms=30000))
```

### Consumer Groups

Consumer groups let many consumers work through a stream, each record leased
to one consumer at a time. A group read leases up to 100 records to its
consumer: the group's ready records first (those whose lease ran out, or that
were nacked), then the stream's records it hasn't read yet. Each record stays
leased until it is acked, nacked, or its lease runs out; one whose lease runs
out is read again, with `deliveries` one higher, so delivery is at least once.
A record nacked or expired `max_deliver` times (5 by default) is dead and
isn't read again.

A group's lease (`ack_timeout_ms`, 5 s to 12 h, 30 s by default) and
`max_deliver` (1 to 255) are fixed when it is created; the first read of a
group that doesn't exist creates it at the defaults. A group name is at most
256 bytes.

```python
from flo import StreamGroupCreateOptions, StreamGroupNackOptions, StreamGroupReadOptions

await client.stream.group_create(
    "events", "processors", StreamGroupCreateOptions(ack_timeout_ms=60_000, max_deliver=3)
)

got = await client.stream.group_read(
    "events", "processors", "worker-1", StreamGroupReadOptions(count=10, block_ms=30000)
)
done, failed = [], []
for record in got.records:  # each carries its deliveries, this one included
    try:
        process(record.payload)
        done.append(record)
    except Exception:
        failed.append(record)

# Ack, nack and touch name the read's delivery: pass the read's result, and
# the records (all of the read's when None).
if done:
    result = await client.stream.group_ack("events", "processors", got, done)
if failed:
    await client.stream.group_nack(
        "events", "processors", got, failed, StreamGroupNackOptions(delay_ms=1000, error="boom")
    )
```

A report the server can't apply to a record (its lease ran out and it was
read again, another consumer holds it, or it was acked already) still
succeeds: that record comes back in `result.refused` with why, and
`result.lost` lists the ones that aren't `GONE` (acked, which is done).
Resending an ack is safe. A handler that needs longer than the lease calls
`group_touch` at least every half lease: with half the lease or less left, the
lease runs a full lease from now, up to 12 h from the read.

```python
info = await client.stream.group_info("events", "processors")  # ready, leased, dead
pending = await client.stream.group_pending("events", "processors")  # leased now
groups = await client.stream.group_list("events")
members = await client.stream.group_members("events", "processors")  # consumers holding records
await client.stream.group_delete("events", "processors")
```

## Action Operations

Actions are registered tasks that can be invoked and executed by workers.

### Register an Action

```python
from flo import ActionRegisterOptions, ActionType

# Register a user-based action (executed by external workers)
await client.action.register(
    "process-image",
    ActionType.USER,
    ActionRegisterOptions(timeout_ms=60000, max_attempts=3)
)
```

`timeout_ms` is the lease: how long a worker holds a run without completing,
failing or touching it. The server takes 5000 to 43200000 ms (0 is its
default, 300000).

### Invoke an Action

```python
from flo import ActionInvokeOptions

# Invoke an action
result = await client.action.invoke(
    "process-image",
    b'{"image_url": "https://example.com/image.jpg"}',
)
print(f"Run ID: {result.run_id}")

# Route the run only to workers registered with matching labels
result = await client.action.invoke(
    "process-image",
    b'{"image_url": "https://example.com/image.jpg"}',
    ActionInvokeOptions(labels={"gpu": True}),
)
```

### Check Action Status

```python
status = await client.action.status(result.run_id)
print(f"Status: {status.status.name}, attempt {status.attempt}")
if status.output is not None:
    print(f"Output: {status.output!r}")
if status.error_message is not None:
    print(f"Error: {status.error_message}")
if status.outcome is not None:  # None for success
    print(f"Outcome: {status.outcome}")

# An action's definition, its runs by status and its completed runs' latency
info = await client.action.info("process-image")
print(info.ready, info.running, info.completed, info.latency_p95_ms)
```

### List and Delete Actions

```python
from flo import ActionListOptions

# List the namespace's actions a page at a time
opts = ActionListOptions(limit=100)
while True:
    page = await client.action.list(opts)
    for action in page.actions:
        print(action.name, action.version, action.timeout_ms)
    if page.cursor is None:
        break
    opts = ActionListOptions(limit=100, cursor=page.cursor)

# Only actions whose names start with a prefix (needs the next server
# release; the current one lists every action)
page = await client.action.list(ActionListOptions(prefix="process-"))

# Delete an action
await client.action.delete("process-image")
```

## Worker Operations

Workers execute tasks for registered actions.

### Register a Worker

```python
# Register a worker that handles specific task types
await client.worker.register(
    "worker-1",
    ["process-image", "resize-image"]
)
```

### Inspect a Worker

```python
# Its row (as client.worker.list shows it), its metadata and its processes
info = await client.worker.info("worker-1")
print(info.worker.status, info.worker.machine_id)
print(info.metadata)  # bytes as registered, or None; decode it if you know it's text
for p in info.processes:  # p.kind: ProcessKind.ACTION or STREAM_CONSUMER
    print(p.name, p.kind.name, p.run_count, p.fail_count, p.last_run_at_ms)
```

List the namespace's workers a page at a time, optionally only those that run an action:

```python
from flo import WorkerListOptions

opts = WorkerListOptions(limit=100, action="process-image")
while True:
    page = await client.worker.list(opts)
    for w in page.workers:
        print(w.worker_id, w.status)
    if page.cursor is None:
        break
    opts = WorkerListOptions(limit=100, action="process-image", cursor=page.cursor)
```

The server allows at most 1024 processes per worker and refuses a registration past its limits; the refusal comes back as a `BadRequestError` carrying its reason (`OUT_OF_RANGE` for too many processes, `FIELD_TOO_LONG` for a process name, the processes together or a machine id too long) and message.

### Await and Process Tasks

A task is one delivery of a run. Its `run`, `deliveries`, `term` and `index`
name the delivery, and every report on it (complete, fail, touch) names them
back; a report for a delivery the run has moved on from raises
`StaleReportError`. A report resent after it applied answers ok.

```python
from flo import WorkerAwaitOptions, WorkerFailOptions

# Wait up to 30 s for a run of either action; the same request_key on a
# retried await gets the task the first one took
result = await client.worker.await_task(
    "worker-1",
    ["process-image", "resize-image"],
    WorkerAwaitOptions(block_ms=30000, request_key=b"poll-1"),
)

if result.task:
    task = result.task
    print(f"Got task {task.task_id}, attempt {task.attempt}, lease {task.lease_ms} ms")

    try:
        output = process(task.input)
        await client.worker.complete("worker-1", task, output)
    except Exception as e:
        # Ready again after delay_ms while attempts are left; retry=False
        # fails the run now, abandoned=True hands it back with no attempt spent
        await client.worker.fail("worker-1", task, str(e), WorkerFailOptions(delay_ms=1000))
```

### Keep a Task

```python
# With half its lease or less left, a touch runs the lease a full lease from
# now (up to 12 h from its delivery); with more left it changes nothing.
# Touch at least every half lease.
expires_at_ms = await client.worker.touch("worker-1", task)
```

### Worker Lifecycle Example

```python
import asyncio
from flo import ActionRegisterOptions, FloClient

async def run_worker():
    async with FloClient("localhost:9000", namespace="myapp") as client:
        # Create an action worker from the client
        worker = client.new_action_worker(concurrency=5, auto_touch=True)

        @worker.action("process-image", ActionRegisterOptions(timeout_ms=60000))
        async def process_image(ctx):
            data = ctx.json()
            result = await do_processing(data)
            return ctx.to_bytes({"status": "done"})

        await worker.start()

asyncio.run(run_worker())
```

The worker registers each action with the server when it starts, with exactly the options given (when none: the server's 300 s lease and 3 attempts). Registering replaces the action's settings, so workers that register one action with different options leave it with whichever registered last.

A run whose handler goes past its lease without touching it is taken back and run again, by this worker or another. `auto_touch=True` touches each task every sixth of its lease, counted from when the task arrived, while the handler runs; it's off by default. A hold ends 12 h after its delivery whatever the touches. Without it, call `await ctx.touch()` from long handlers. The worker also stops any handler after its own `action_timeout` (300 s by default) whatever the action's `timeout_ms`, and fails the run with retry, so raise it for actions that run longer.

Delivery is at-least-once: a result lost to a dropped connection is resent, and one that never arrives lets the lease run out, so a handler may see the same task or record again. A handler that raises (`asyncio.CancelledError` included) or runs past `action_timeout` fails the run with retry, so it runs again until its attempts are spent; raise `NonRetryableError` to fail it for good.

`worker.stop()` takes no new tasks and doesn't cancel running handlers: they finish (up to `action_timeout`), their results are sent, and the worker deregisters before `start()` returns. While the worker is stopping, a handler's cancellation abandons its run: the run is handed back untried and spends no attempt.

Both workers register in the worker registry and heartbeat every 30 s, reporting the handlers they are running. One the server removed after missed heartbeats registers again, with the same options, when its next heartbeat is answered not found.

`worker.drain()` stops taking work and lets the running handlers finish, heartbeats going on; the server is told, so it assigns the worker nothing more, and `start()` returns once the last handler has reported. A drain started on the server does the same. Neither waits out `block_ms`: `drain()` interrupts the worker's own poll connection, so an action worker's held await and a stream worker's held read end at once. A drain started on the server answers an action worker's held await at once, refused as `WORKER_DRAINING`; it doesn't wake a stream worker's held group read, so a stream worker learns of it from its next heartbeat (every 30 s) and only then interrupts its read and leaves. `worker.stop()` ends polling at once, also mid-reconnect or mid-drain; running handlers still finish, and a stream worker hands back the records of its last batch it hasn't started. Either one before or during `start()` ends that run, and a worker can be started again afterwards.

### StreamWorker Example

```python
import asyncio
from flo import FloClient, StreamContext

async def process_event(ctx: StreamContext) -> None:
    event = ctx.json()
    print(f"Got event: {event}")
    # Return normally → auto-ack
    # Raise an exception → auto-nack (redelivery)

async def run_stream_worker():
    async with FloClient("localhost:9000", namespace="myapp") as client:
        worker = client.new_stream_worker(
            stream="events",
            group="processors",
            handler=process_event,
            concurrency=5,
            ack_timeout_ms=60_000,  # creates the group with this lease
            auto_touch=True,        # keeps a record while its handler runs
        )
        await worker.start()

asyncio.run(run_stream_worker())
```

A stream worker acks each record its handler returns from and nacks each one
its handler raises on, naming the read's delivery. A record whose handler runs
past the group's lease is read again elsewhere unless `auto_touch=True`, which
touches it every sixth of the lease (at least 100 ms apart) counted from when
its read came in. The worker learns the lease when it creates the group or
finds it listed; until then (a group its first read makes) it paces by the
shortest lease a group may have, 5 s. With `ack_timeout_ms` or `max_deliver` set, the worker
creates the group at start and fails if it exists with other settings; with
neither, it takes the group as it is. A worker that loses its connection
reconnects and reads on: the records it held come back once their lease runs
out.

## Configuration

### Client Options

```python
client = FloClient(
    "localhost:9000",
    namespace="myapp",      # Default namespace for all operations
    timeout_ms=5000,        # Connection and operation timeout
    debug=True,             # Enable debug logging
    retry=RetryPolicy(),    # Retries of refused, not-run requests (the default)
)
```

A call that times out or is cancelled drops the connection; call `reconnect()` before reusing the client (workers do this themselves).

### Namespaces

Each operation can override the default namespace:

```python
from flo import GetOptions, PutOptions, EnqueueOptions

# Use client's default namespace
await client.kv.put("key", b"value")

# Override namespace for this operation
await client.kv.put("key", b"value", PutOptions(namespace="other"))
await client.kv.get("key", GetOptions(namespace="other"))
await client.queue.enqueue("tasks", payload, EnqueueOptions(namespace="other"))
```

## Error Handling

```python
from flo import (
    FloError,
    NotFoundError,
    ConflictError,
    ConnectionFailedError,
)

try:
    await client.kv.put("key", b"new", PutOptions(cas_version=1))
except ConflictError:
    print("CAS version mismatch - value was modified")
except ConnectionFailedError:
    print("Failed to connect to server")
except FloError as e:
    print(f"Flo error: {e}")
```

A call that times out (`RequestTimeoutError`, also an `asyncio.TimeoutError`) or is cancelled drops the connection; call `reconnect()` before reusing the client (workers do this themselves).

### Error Types

| Error | Description |
|-------|-------------|
| `NotFoundError` | Key or resource not found |
| `NoNamespaceError` | Status `NO_NAMESPACE` (14), not a `NotFoundError`: a read, list or scan in a namespace that was never created (a write creates it) |
| `BadRequestError` | Invalid request parameters |
| `ConflictError` | CAS version mismatch, a failed condition, and the like (the reason says which) |
| `StaleReportError` | A `ConflictError` with reason `STALE_REPORT`: an action complete, fail or touch for a delivery the run has moved on from (the message says how) |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server is overloaded, and the client's retries ran out |
| `UnavailableError` | No leader, or the shard isn't taking writes or is offline, and the client's retries ran out (the message says why, and whether an operator must act first) |
| `StaleNotFoundError` | `kv.get` or `kv.json_get` found no key, but a halted or read-blocked shard answered: absent only as of the entry it couldn't apply |
| `LimitError` | Status `LIMIT` (15): the node is at one of its limits; `reason` says which (`TOO_MANY_ACTIONS`, `MEMORY_BUDGET`, `TOO_MANY_WORKFLOW_DEFINITIONS`, `TOO_MANY_WORKFLOW_RUNS`). It didn't run. Terminal, never retried (see below) |
| `OutcomePendingError` | The write committed but hasn't applied: its shard halted at or before it (reason `SHARD_HALTED`, ran `UNKNOWN`; the message says whether it applies when the shard resumes). Never retried |
| `InternalServerError` | Server internal error; not retryable (may mean committed but not applied) |
| `GenericServerError` | Any other error status, including one this SDK doesn't know (`status_code` carries it) |
| `ConnectionFailedError` | Failed to connect |
| `RequestTimeoutError` | No response within the timeout |
| `InvalidChecksumError` | Response CRC32C mismatch |

### Retries

When the server refuses a request as `overloaded` or `unavailable` and says it
didn't run it (`error.ran == Ran.NO`), the client sends it again itself, as a
new request, after a jittered wait. Because the server never ran it, this is
safe for writes too. That covers a leader that isn't ready yet (reason
`LEADER_UNKNOWN`), including a node catching up after a restart, and a
stream read whose records are still being read back from disk. Any other
refusal, or one that may have run, comes straight back.

`UnavailableError` with ran `NO` is routine, not a fault: a first read of
records that left the warm store is answered so while the shard reads them
back from disk. A group read refused this way leased nothing, so sending the
same one again is safe; the stream worker's read loop does so with a backoff,
without logging an error.

A `LimitError` is never retried: the node is at one of its limits
(today, registering past the 10,000 actions a shard holds), and retrying won't
clear it. It isn't permanent either: deleting something, giving the node more
room, or raising the setting does. Workers don't stop on one: their
registration, poll and touch loops log it once per reason, at error, and back
off (up to 30 s) until it clears.

Two refusals from a halted shard are never retried, and come back after one
attempt for the caller to decide:

- `OutcomePendingError`: the write committed, so resending it would write it
  twice, or halt the shard again.
- `UnavailableError` with `error.reason == Reason.SHARD_HALTED`: the shard
  takes no writes until an operator repairs it. The reason tells it apart from
  a leader change.

A halted shard still answers reads, from its state just before the entry it
halted at, and so does one blocked reading its next entry from disk (it holds
that entry unapplied and retries the read). Such a read succeeds; its result says so with `stale=True` (on
`GetResult`, `GetJsonResult`, each `MGetEntry`, `ScanResult`, `HistoryResult`,
`ExistsResult`, and the raw response). It is not an error. A key absent on
such an answer is absent only as of that entry, so `kv.get` and `kv.json_get`
raise `StaleNotFoundError` (a `NotFoundError` with `stale` True) instead of
returning `None`; `kv.history` returns an empty page with `stale=True`, and
`kv.exists` and `kv.mget` entries say `stale` too. A refusal from a halted
shard carries `error.stale` as well. Reads other than KV ones (streams,
queues, workers and the rest) don't surface the flag yet; they drop it this
release.

| `RetryPolicy` field | Default | |
|---|---|---|
| `max_attempts` | 4 | Tries in all, the first included; 1 turns retrying off |
| `initial_backoff_ms` | 50 | Ceiling of the first wait, in ms; it doubles each retry |
| `max_backoff_ms` | 1000 | Largest ceiling, in ms |

Each wait is uniform between 0 and its ceiling, so with the defaults a refused
request is given back after at most about 350 ms of waiting. A negative backoff
counts as 0, and a `max_backoff_ms` below `initial_backoff_ms` as
`initial_backoff_ms`. The request goes again on the same connection; the server
routes it afresh. Other calls on the client are not held up by the wait.

The action and stream workers use their client's policy, and wait the same way
between failed polls, from 100 ms up to 5 s, starting again from 100 ms after a
poll that succeeds; `stop()` cuts the wait short. A stream worker retries a
group create refused this way instead of stopping.

Workers are the exception to the halted-shard rule: registering, polling and
creating a group are safe to repeat, so on `SHARD_HALTED` either worker's
registration at start and poll, and a stream worker's group create, keep
trying, waiting from 100 ms up to 30 s, logging each refusal at
warning with the server's message. A worker started during a halt comes up,
and one running through it resumes, by itself once the shard is repaired; no
restart is needed. They wait only on a write the shard refused without running it
(`UNAVAILABLE`, ran `NO`); an `OutcomePendingError` (an action registration or
group create that committed but hasn't applied) ends `start()` after one try,
with the server's message, since sending it again could halt the shard again.
Results and acks the shard refuses are not resent.

```python
from flo import FloClient, RetryPolicy

# Up to 8 tries, waiting at most 2 s between them
client = FloClient("localhost:9000", retry=RetryPolicy(
    max_attempts=8, initial_backoff_ms=100, max_backoff_ms=2000,
))

# No retries
client = FloClient("localhost:9000", retry=RetryPolicy(max_attempts=1))
```

## Requirements

- Python 3.10+
- asyncio

## License

Apache-2.0 - see [LICENSE](LICENSE) for details.
