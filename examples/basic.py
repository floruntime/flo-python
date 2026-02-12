#!/usr/bin/env python3
"""Basic example of using the Flo Python SDK.

This example demonstrates:
- Connecting to a Flo server
- KV operations (get, put, delete, scan)
- Queue operations (enqueue, dequeue, ack)
"""

import asyncio
import sys

from flo import (
    DequeueOptions,
    EnqueueOptions,
    FloClient,
    NotFoundError,
    PutOptions,
    ScanOptions,
)


async def kv_example(client: FloClient) -> None:
    """Demonstrate KV operations."""
    print("\n=== KV Operations ===\n")

    # Put a value
    print("Putting 'user:1' = 'Alice'")
    await client.kv.put("user:1", b"Alice")

    # Put with TTL
    print("Putting 'session:abc' with 60s TTL")
    await client.kv.put("session:abc", b"session-data", PutOptions(ttl_seconds=60))

    # Get a value
    value = await client.kv.get("user:1")
    if value:
        print(f"Got 'user:1' = '{value.decode()}'")

    # Get non-existent key
    value = await client.kv.get("user:999")
    if value is None:
        print("'user:999' not found (expected)")

    # Put more values for scan
    await client.kv.put("user:2", b"Bob")
    await client.kv.put("user:3", b"Charlie")

    # Scan with prefix
    print("\nScanning keys with prefix 'user:'")
    result = await client.kv.scan("user:", ScanOptions(limit=10))
    for entry in result.entries:
        print(f"  {entry.key.decode()} = {entry.value.decode() if entry.value else '(no value)'}")

    # Delete a key
    print("\nDeleting 'user:2'")
    await client.kv.delete("user:2")

    # Verify deletion
    value = await client.kv.get("user:2")
    if value is None:
        print("'user:2' deleted successfully")

    # Clean up
    await client.kv.delete("user:1")
    await client.kv.delete("user:3")
    await client.kv.delete("session:abc")


async def queue_example(client: FloClient) -> None:
    """Demonstrate Queue operations."""
    print("\n=== Queue Operations ===\n")

    queue_name = "example-tasks"

    # Enqueue messages
    print("Enqueueing messages...")
    seq1 = await client.queue.enqueue(queue_name, b'{"task": "task1"}')
    print(f"  Enqueued task1 with seq={seq1}")

    seq2 = await client.queue.enqueue(
        queue_name,
        b'{"task": "task2"}',
        EnqueueOptions(priority=10),  # Higher priority
    )
    print(f"  Enqueued task2 (priority=10) with seq={seq2}")

    seq3 = await client.queue.enqueue(queue_name, b'{"task": "task3"}')
    print(f"  Enqueued task3 with seq={seq3}")

    # Dequeue messages
    print("\nDequeueing messages...")
    result = await client.queue.dequeue(
        queue_name,
        10,
        DequeueOptions(visibility_timeout_ms=30000),
    )

    print(f"  Received {len(result.messages)} messages")
    for msg in result.messages:
        print(f"    seq={msg.seq}: {msg.payload.decode()}")

    # Acknowledge messages
    if result.messages:
        seqs = [msg.seq for msg in result.messages]
        print(f"\nAcknowledging messages: {seqs}")
        await client.queue.ack(queue_name, seqs)
        print("  Messages acknowledged")


async def main() -> None:
    """Main entry point."""
    # Default to localhost:9000, or use command line argument
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "localhost:9000"

    print(f"Connecting to Flo server at {endpoint}...")

    try:
        async with FloClient(endpoint, namespace="example", debug=False) as client:
            print("Connected!")

            await kv_example(client)
            await queue_example(client)

            print("\n=== Done ===")

    except ConnectionRefusedError:
        print(f"Error: Could not connect to {endpoint}")
        print("Make sure the Flo server is running.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
