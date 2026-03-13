"""Example: StreamWorker usage with the Flo Python SDK

Demonstrates how to use StreamWorker to process stream records
via consumer groups with automatic ack/nack handling.
"""

import asyncio
import logging
import os
import signal

from flo import FloClient, StreamContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def process_event(ctx: StreamContext) -> None:
    """Process a stream record.

    Return normally to auto-ack. Raise an exception to auto-nack
    (the record will be redelivered).
    """
    event = ctx.json()
    logger.info(
        f"Processing event (stream={ctx.stream}, id={ctx.stream_id}): {event}"
    )

    # Simulate work
    await asyncio.sleep(0.1)

    # If processing fails, just raise — the worker will nack for you:
    # raise RuntimeError("transient failure")


async def main():
    client = FloClient(
        os.getenv("FLO_ENDPOINT", "localhost:3000"),
        namespace=os.getenv("FLO_NAMESPACE", "myapp"),
        debug=os.getenv("FLO_DEBUG", "").lower() in ("1", "true"),
    )
    await client.connect()

    worker = client.new_stream_worker(
        stream="events",
        group="processors",
        handler=process_event,
        concurrency=5,
        batch_size=10,
    )

    # Handle shutdown signals
    def signal_handler():
        logger.info("Received shutdown signal")
        worker.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    logger.info("Starting stream worker...")
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        await worker.close()
        await client.close()
        logger.info("Stream worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
