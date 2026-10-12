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
- **Queues**: Priority task queues (currently at-most-once: dequeue hands each message out once)
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

# Names of keys with a prefix ("" for all), up to 100 per page by default
result = await client.kv.scan("user:")
for name in result.names:
    print(name)

# Next pages: the cursor is None on the last page (limit is 1..1024)
while result.cursor is not None:
    result = await client.kv.scan("user:", ScanOptions(cursor=result.cursor, limit=100))
```

### MGet

```python
# One entry per key, in order (at most 256 keys)
for e in await client.kv.mget(["user:1", "user:2"]):
    if e.found:
        print(e.key, e.version, e.value)
```

### History

```python
from flo import HistoryOptions

# Versions newest first (limit 1..65, default 10), each with its write time in ms
history = await client.kv.history("user:123", HistoryOptions(limit=10))
for entry in history:
    print(f"v{entry.version} at {entry.at_ms}: {entry.value.decode()}")
```

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

if await client.kv.exists("session:abc"):
    ...
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
# Every op inside is routed by the transaction's routing key
async with await client.kv.begin("user:123") as txn:
    await txn.put("user:123:name", b"Jane")
    await txn.incr("user:123:visits")
    await txn.expire("user:123:session", 60_000)
    result = await txn.commit()
    print(result.commit_index, result.op_count)
```

## Queue Operations

### Enqueue

```python
from flo import EnqueueOptions

# Simple enqueue
seq = await client.queue.enqueue("tasks", b'{"task": "process"}')

# With priority (0-255, lower is taken first; default 0)
seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10))
```

### Dequeue

Queues are currently at-most-once: a dequeue acknowledges each message as it
hands it out. A message is not redelivered in normal operation, even if the
consumer crashes while handling it, so handle failures in your own code.

```python
from flo import DequeueOptions

# Dequeue up to 10 messages
result = await client.queue.dequeue("tasks", 10)
for msg in result.messages:
    print(f"Processing message {msg.seq}: {msg.payload}")

# Long polling (wait up to 30s for messages)
result = await client.queue.dequeue("tasks", 10, DequeueOptions(block_ms=30000))
```

### Ack/Nack and the Dead Letter Queue

`queue.ack`, `queue.nack`, `queue.dlq_list` and `queue.dlq_requeue` exist, but
while queues are at-most-once they do nothing useful: a dequeued message is
already acknowledged, so ack and nack have no effect on it, nack does not
retry it, and messages don't reach the DLQ in normal use.

### Peek

```python
# Peek at messages without removing them
result = await client.queue.peek("tasks", 5)
for msg in result.messages:
    print(f"Message {msg.seq}: {msg.payload}")
# Messages remain visible to other consumers
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
from flo import StreamReadOptions, StreamStartMode

# Read from beginning (default)
result = await client.stream.read("events")
for record in result.records:
    print(f"offset={record.offset} ts={record.timestamp}: {record.payload}")

# Read from specific offset
result = await client.stream.read(
    "events",
    StreamReadOptions(start_mode=StreamStartMode.OFFSET, offset=100, count=10)
)

# Read from tail (latest records)
result = await client.stream.read(
    "events",
    StreamReadOptions(start_mode=StreamStartMode.TAIL, count=10)
)

# Read from timestamp
result = await client.stream.read(
    "events",
    StreamReadOptions(start_mode=StreamStartMode.TIMESTAMP, timestamp=1699999999000)
)

# Blocking read (long polling) - wait up to 30s for new records
result = await client.stream.read(
    "events",
    StreamReadOptions(offset=100, block_ms=30000)
)
```

### Consumer Groups

Consumer groups allow multiple consumers to process a stream in parallel, with each record delivered to only one consumer.

```python
from flo import StreamGroupReadOptions

# Join a consumer group
await client.stream.group_join("events", "processors", "worker-1")

# Read from consumer group
result = await client.stream.group_read("events", "processors", "worker-1")
for record in result.records:
    try:
        process(record.payload)
        # Acknowledge successful processing
        await client.stream.group_ack("events", "processors", [record.id])
    except Exception:
        # Record will be redelivered to another consumer
        pass

# With blocking (long polling)
result = await client.stream.group_read(
    "events", "processors", "worker-1",
    StreamGroupReadOptions(count=10, block_ms=30000)
)
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
    ActionRegisterOptions(timeout_ms=60000, max_retries=3)
)
```

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
print(f"Status: {status.status}")
if status.result:
    print(f"Result: {status.result}")
```

### List and Delete Actions

```python
from flo import ActionListOptions

# List all actions
actions = await client.action.list()

# List with prefix filter
actions = await client.action.list(ActionListOptions(prefix="process-"))

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

### Await and Process Tasks

```python
from flo import WorkerAwaitOptions

# Wait for a task (blocking)
result = await client.worker.await_task(
    "worker-1",
    ["process-image", "resize-image"],
    WorkerAwaitOptions(block_ms=30000)  # Wait up to 30s
)

if result.task:
    task = result.task
    print(f"Got task: {task.task_id} for action: {task.action_name}")

    try:
        # Process the task
        output = process(task.input)

        # Complete successfully
        await client.worker.complete("worker-1", task.task_id, output)
    except Exception as e:
        # Fail the task (will be retried)
        await client.worker.fail("worker-1", task.task_id, str(e))
```

### Extend Task Lease

```python
from flo import WorkerTouchOptions

# For long-running tasks, extend the lease periodically
await client.worker.touch(
    "worker-1",
    task.task_id,
    WorkerTouchOptions(extend_ms=30000)
)
```

### Worker Lifecycle Example

```python
import asyncio
from flo import FloClient

async def run_worker():
    async with FloClient("localhost:9000", namespace="myapp") as client:
        # Create an action worker from the client
        worker = client.new_action_worker(concurrency=5)

        @worker.action("process-image")
        async def process_image(ctx):
            data = ctx.json()
            result = await do_processing(data)
            return ctx.to_bytes({"status": "done"})

        await worker.start()

asyncio.run(run_worker())
```

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
        )
        await worker.start()

asyncio.run(run_stream_worker())
```

## Configuration

### Client Options

```python
client = FloClient(
    "localhost:9000",
    namespace="myapp",      # Default namespace for all operations
    timeout_ms=5000,        # Connection and operation timeout
    debug=True,             # Enable debug logging
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
| `BadRequestError` | Invalid request parameters |
| `ConflictError` | CAS version mismatch |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server is overloaded; retryable |
| `UnavailableError` | No leader, or the shard isn't taking writes or is offline; retryable (the message says why) |
| `InternalServerError` | Server internal error; not retryable (may mean committed but not applied) |
| `GenericServerError` | Any other error status, including one this SDK doesn't know (`status_code` carries it) |
| `ConnectionFailedError` | Failed to connect |
| `RequestTimeoutError` | No response within the timeout |
| `InvalidChecksumError` | Response CRC32 mismatch |

## Requirements

- Python 3.10+
- asyncio

## License

Apache-2.0 - see [LICENSE](LICENSE) for details.
