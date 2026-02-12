"""Example: High-level Worker API usage with the Flo Python SDK

This example demonstrates how to use the Worker class to process actions.
"""

import asyncio
import logging
import os
import signal
from dataclasses import dataclass

from flo import ActionContext, Worker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Data Types
# =============================================================================


@dataclass
class OrderItem:
    sku: str
    quantity: int
    price: float


@dataclass
class OrderRequest:
    order_id: str
    customer_id: str
    amount: float
    items: list[dict]  # Will be converted to OrderItem


@dataclass
class OrderResult:
    order_id: str
    status: str
    processed_by: str


@dataclass
class NotificationRequest:
    user_id: str
    channel: str
    message: str


# =============================================================================
# Action Handlers
# =============================================================================


async def process_order(ctx: ActionContext) -> bytes:
    """Process an order - demonstrates long-running tasks with Touch."""
    # Parse input
    data = ctx.json()
    req = OrderRequest(
        order_id=data["order_id"],
        customer_id=data["customer_id"],
        amount=data["amount"],
        items=data.get("items", []),
    )

    logger.info(
        f"Processing order {req.order_id} for customer {req.customer_id} "
        f"(amount: ${req.amount:.2f})"
    )

    # Simulate a long-running order processing task
    # For long tasks, periodically call Touch to extend the lease
    items = [OrderItem(**item) for item in req.items]

    for i, item in enumerate(items):
        logger.info(
            f"  Processing item {i + 1}/{len(items)}: {item.sku} (qty: {item.quantity})"
        )

        # Simulate work for each item
        await asyncio.sleep(2)

        # Extend the lease every few items to prevent timeout
        # This is critical for long-running tasks
        if (i + 1) % 3 == 0:
            try:
                await ctx.touch(30000)
                logger.info(f"  Extended lease for order {req.order_id}")
            except Exception as e:
                logger.warning(f"Warning: failed to extend lease: {e}")

    # Return result
    result = OrderResult(
        order_id=req.order_id,
        status="processed",
        processed_by=ctx.task_id,
    )
    return ctx.to_bytes(result.__dict__)


async def send_notification(ctx: ActionContext) -> bytes:
    """Send a notification - demonstrates simple action."""
    data = ctx.json()
    req = NotificationRequest(**data)

    logger.info(
        f"Sending {req.channel} notification to user {req.user_id}: {req.message}"
    )

    # Simulate sending notification
    await asyncio.sleep(0.5)

    return ctx.to_bytes({
        "success": True,
        "channel": req.channel,
        "user_id": req.user_id,
    })


async def generate_report(ctx: ActionContext) -> bytes:
    """Generate a report - demonstrates context usage."""
    data = ctx.json()
    report_type = data.get("type", "summary")
    date_range = data.get("date_range", "last_7_days")

    logger.info(
        f"Generating {report_type} report for {date_range} "
        f"(attempt {ctx.attempt}, task {ctx.task_id})"
    )

    # Simulate report generation with progress
    total_steps = 5
    for step in range(total_steps):
        logger.info(f"  Report generation step {step + 1}/{total_steps}")
        await asyncio.sleep(1)

        # Extend lease periodically
        if step == 2:
            await ctx.touch(30000)

    return ctx.to_bytes({
        "report_type": report_type,
        "date_range": date_range,
        "generated_at": ctx.created_at,
        "rows": 1500,
    })


# =============================================================================
# Main
# =============================================================================


async def main():
    # Create worker with configuration
    worker = Worker(
        endpoint=os.getenv("FLO_ENDPOINT", "localhost:3000"),
        namespace=os.getenv("FLO_NAMESPACE", "myapp"),
        concurrency=5,
        action_timeout=300,  # 5 minutes
        debug=os.getenv("FLO_DEBUG", "").lower() in ("1", "true"),
    )

    # Register action handlers using register_action()
    worker.register_action("process-order", process_order)
    worker.register_action("send-notification", send_notification)
    worker.register_action("generate-report", generate_report)

    # Register using decorator syntax
    @worker.action("health-check")
    async def health_check(ctx: ActionContext) -> bytes:
        """Simple health check action."""
        return ctx.to_bytes({
            "status": "healthy",
            "worker_id": ctx.task_id,
            "timestamp": ctx.created_at,
        })

    # Handle shutdown signals
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        worker.stop()
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    # Start worker (blocks until stopped)
    logger.info("Starting worker...")
    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        await worker.close()
        logger.info("Worker shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
