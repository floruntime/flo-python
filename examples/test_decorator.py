"""Minimal test: register an action using only the @worker.action decorator."""
import asyncio
import logging
import signal
from flo import FloClient, ActionContext

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def main():
    client = FloClient("localhost:9000", namespace="default", debug=True)
    await client.connect()
    print(f"Connected: {client.is_connected}")

    worker = client.new_action_worker(concurrency=1)

    @worker.action("health-check")
    async def health_check(ctx: ActionContext) -> bytes:
        return ctx.to_bytes({"status": "healthy"})

    stop = asyncio.Event()

    def on_signal():
        print("Shutting down...")
        worker.stop()
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, on_signal)

    print("Starting worker with @worker.action('health-check')...")
    try:
        await worker.start()
    except KeyboardInterrupt:
        pass
    finally:
        await worker.close()
        await client.close()
        print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
