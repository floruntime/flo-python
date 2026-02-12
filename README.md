# flo-python

Python SDK for the [Flo](https://github.com/floruntime) distributed systems platform.

## Installation

```bash
pip install flo
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
- **Queues**: Priority-based task queues with visibility timeout and dead letter queues
- **Streams**: Append-only logs with consumer groups for distributed processing
- **Actions**: Registered tasks with configurable timeouts, retries, and idempotency
- **Workers**: Distributed task execution with lease management and heartbeats
- **Async/await**: Native asyncio support for high-performance applications
- **Type hints**: Full type annotations for IDE support

## KV Operations

### Get

```python
from flo import GetOptions

# Simple get
value = await client.kv.get("key")
if value is not None:
    print(value.decode())

# Blocking get - wait up to 5 seconds for value to appear
value = await client.kv.get("key", GetOptions(block_ms=5000))

# Blocking get - wait indefinitely (0 = infinite)
value = await client.kv.get("key", GetOptions(block_ms=0))

# Get with namespace override
value = await client.kv.get("key", GetOptions(namespace="other"))
```

### Put

```python
from flo import PutOptions

# Simple put
await client.kv.put("key", b"value")

# Put with TTL (expires in 1 hour)
await client.kv.put("session:abc", b"data", PutOptions(ttl_seconds=3600))

# Put with CAS (optimistic locking)
await client.kv.put("counter", b"2", PutOptions(cas_version=1))

# Put only if key doesn't exist
await client.kv.put("key", b"value", PutOptions(if_not_exists=True))

# Put only if key exists
await client.kv.put("key", b"value", PutOptions(if_exists=True))
```

### Delete

```python
await client.kv.delete("key")
```

### Scan

```python
from flo import ScanOptions

# Scan all keys with prefix
result = await client.kv.scan("user:")
for entry in result.entries:
    print(f"{entry.key.decode()}: {entry.value.decode()}")

# Paginated scan
result = await client.kv.scan("user:", ScanOptions(limit=100))
while result.has_more:
    # Process entries...
    result = await client.kv.scan("user:", ScanOptions(cursor=result.cursor, limit=100))

# Keys only (more efficient when you don't need values)
result = await client.kv.scan("user:", ScanOptions(keys_only=True))
```

### History

```python
from flo import HistoryOptions

# Get version history for a key
history = await client.kv.history("user:123", HistoryOptions(limit=10))
for entry in history:
    print(f"v{entry.version} at {entry.timestamp}: {entry.value.decode()}")
```

## Queue Operations

### Enqueue

```python
from flo import EnqueueOptions

# Simple enqueue
seq = await client.queue.enqueue("tasks", b'{"task": "process"}')

# With priority (higher = more urgent, 0-255)
seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(priority=10))

# With delay (message invisible for 60 seconds)
seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(delay_ms=60000))

# With deduplication key
seq = await client.queue.enqueue("tasks", payload, EnqueueOptions(dedup_key="task-123"))
```

### Dequeue

```python
from flo import DequeueOptions

# Dequeue up to 10 messages
result = await client.queue.dequeue("tasks", 10)
for msg in result.messages:
    print(f"Processing message {msg.seq}: {msg.payload}")

# Long polling (wait up to 30s for messages)
result = await client.queue.dequeue("tasks", 10, DequeueOptions(block_ms=30000))

# Custom visibility timeout (message invisible for 60s)
result = await client.queue.dequeue("tasks", 10, DequeueOptions(visibility_timeout_ms=60000))
```

### Ack/Nack

```python
from flo import NackOptions

result = await client.queue.dequeue("tasks", 10)
for msg in result.messages:
    try:
        process(msg.payload)
        # Acknowledge successful processing
        await client.queue.ack("tasks", [msg.seq])
    except Exception:
        # Retry the message
        await client.queue.nack("tasks", [msg.seq])
        # Or send to DLQ
        await client.queue.nack("tasks", [msg.seq], NackOptions(to_dlq=True))
```

### Dead Letter Queue

```python
from flo import DlqListOptions

# List DLQ messages
result = await client.queue.dlq_list("tasks", DlqListOptions(limit=100))
for msg in result.messages:
    print(f"Failed message {msg.seq}: {msg.payload}")

# Requeue DLQ messages
seqs = [msg.seq for msg in result.messages]
await client.queue.dlq_requeue("tasks", seqs)
```

### Peek

```python
# Peek at messages without creating leases (no visibility timeout)
result = await client.queue.peek("tasks", 5)
for msg in result.messages:
    print(f"Message {msg.seq}: {msg.payload}")
# Messages remain visible to other consumers
```

### Touch (Lease Renewal)

```python
# Renew lease on messages during long-running processing
result = await client.queue.dequeue("tasks", 1)
msg = result.messages[0]

# Long running task - periodically touch to prevent visibility timeout
for chunk in process_in_chunks(msg.payload):
    await process_chunk(chunk)
    await client.queue.touch("tasks", [msg.seq])  # Renew lease

await client.queue.ack("tasks", [msg.seq])
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
        await client.stream.group_ack("events", "processors", [record.offset])
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
    ActionInvokeOptions(priority=10)
)
print(f"Run ID: {result.run_id}")

# Invoke with idempotency key (prevents duplicate runs)
result = await client.action.invoke(
    "process-image",
    payload,
    ActionInvokeOptions(idempotency_key="order-123-image")
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
from flo import FloClient, WorkerAwaitOptions, WorkerFailOptions

async def run_worker():
    async with FloClient("localhost:9000") as client:
        worker_id = "worker-1"
        task_types = ["process-image"]

        # Register the worker
        await client.worker.register(worker_id, task_types)

        while True:
            # Wait for tasks
            result = await client.worker.await_task(
                worker_id,
                task_types,
                WorkerAwaitOptions(block_ms=30000)
            )

            if not result.task:
                continue

            task = result.task
            try:
                # Process task
                output = await process_image(task.input)
                await client.worker.complete(worker_id, task.task_id, output)
            except Exception as e:
                await client.worker.fail(
                    worker_id,
                    task.task_id,
                    str(e),
                    WorkerFailOptions(retry=True)
                )

asyncio.run(run_worker())
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

### Error Types

| Error | Description |
|-------|-------------|
| `NotFoundError` | Key or resource not found |
| `BadRequestError` | Invalid request parameters |
| `ConflictError` | CAS version mismatch |
| `UnauthorizedError` | Authentication failed |
| `OverloadedError` | Server is overloaded |
| `InternalServerError` | Server internal error |
| `ConnectionFailedError` | Failed to connect |
| `InvalidChecksumError` | Response CRC32 mismatch |

## Requirements

- Python 3.10+
- asyncio

## License

MIT
