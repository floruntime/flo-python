"""Shared fixtures for Flo SDK E2E tests.

Requires a running Flo server. Set FLO_ENDPOINT env var (default: localhost:4453).
"""

import asyncio
import json
import os

import pytest
import pytest_asyncio

from flo import ActionContext, ActionResult, FloClient
from flo.worker import ActionWorker

FLO_ENDPOINT = os.environ.get("FLO_ENDPOINT", "localhost:4453")


# ---------------------------------------------------------------------------
# Action handlers — mirrors Go examples/workflows/workflows_test.go
# ---------------------------------------------------------------------------

def _safe_json(ctx: ActionContext) -> dict:
    """Parse JSON payload, returning empty dict on empty/invalid payload."""
    if not ctx.payload:
        return {}
    try:
        data = json.loads(ctx.payload.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}


async def validate_order(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    order_id = data.get("orderId", "")
    amount = data.get("amount", 0)
    if not order_id:
        raise ValueError("missing orderId")
    if amount > 2000:
        raise ValueError(f"amount ${amount} exceeds limit")
    return ctx.to_bytes({"valid": True, "orderId": order_id})


async def charge_payment(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    amount = data.get("amount", 0)
    if amount > 1500:
        raise ValueError("card declined")
    return ctx.to_bytes({"charged": True, "amount": amount})


async def ship_order(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    order_id = data.get("orderId", "")
    if order_id.endswith("-FAIL"):
        raise ValueError("shipping failed")
    return ctx.to_bytes({"shipped": True, "trackingId": f"TRK-{order_id}"})


async def validate_expense(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"valid": True})


async def process_expense(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"processed": True, "status": "complete"})


async def review_order(ctx: ActionContext) -> ActionResult:
    data = _safe_json(ctx)
    amount = data.get("amount", 0)
    if amount < 100:
        return ctx.result("approved", {"approved": True})
    elif amount >= 500:
        return ctx.result("rejected", {"reason": "amount too high"})
    else:
        return ctx.result("needs_review", {"queued": True})


async def fulfill_order(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"fulfilled": True})


async def notify_rejection(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"notified": True})


async def manual_review(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"queued": True})


async def validate_payment(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    amount = data.get("amount", 0)
    if data and amount <= 0:
        raise ValueError("invalid amount")
    return ctx.to_bytes({"valid": True, "amount": amount})


async def process_payment(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    amount = data.get("amount", 0)
    order_id = data.get("orderId", "")
    if amount > 5000:
        raise ValueError("amount exceeds processor limit")
    if order_id.startswith("FB-"):
        raise ValueError("simulated processor failure")
    return ctx.to_bytes({"transactionId": f"TXN-{order_id}", "amount": amount})


async def process_payment_fallback(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    amount = data.get("amount", 0)
    order_id = data.get("orderId", "")
    if amount > 5000:
        raise ValueError("amount exceeds fallback limit")
    return ctx.to_bytes({"transactionId": f"FB-TXN-{order_id}", "amount": amount})


async def send_confirmation(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"sent": True, "channel": "email"})


async def send_rejection(ctx: ActionContext) -> bytes:
    data = _safe_json(ctx)
    return ctx.to_bytes({"sent": True, "reason": data.get("reason", "rejected")})


async def reconcile_spend(ctx: ActionContext) -> bytes:
    return ctx.to_bytes({"budgets_checked": 3, "discrepancies_corrected": 0})


def register_all_actions(worker: ActionWorker) -> None:
    """Register all action handlers on the worker."""
    worker.register_action("validate-order", validate_order)
    worker.register_action("charge-payment", charge_payment)
    worker.register_action("ship-order", ship_order)
    worker.register_action("validate-expense", validate_expense)
    worker.register_action("process-expense", process_expense)
    worker.register_action("review-order", review_order)
    worker.register_action("fulfill-order", fulfill_order)
    worker.register_action("notify-rejection", notify_rejection)
    worker.register_action("manual-review", manual_review)
    worker.register_action("validate-payment", validate_payment)
    worker.register_action("process-payment", process_payment)
    worker.register_action("process-payment-fallback", process_payment_fallback)
    worker.register_action("send-confirmation", send_confirmation)
    worker.register_action("send-rejection", send_rejection)
    worker.register_action("reconcile-spend", reconcile_spend)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def client():
    """Session-scoped Flo client connected to the test server."""
    c = FloClient(FLO_ENDPOINT, namespace="default", timeout_ms=35000)
    await c.connect()
    yield c
    await c.close()


@pytest_asyncio.fixture(scope="session")
async def worker(client: FloClient):
    """Session-scoped ActionWorker running in the background."""
    w = client.new_action_worker(concurrency=5, block_ms=5000)
    register_all_actions(w)
    task = asyncio.create_task(w.start())
    # Give the worker time to connect & register
    await asyncio.sleep(1.0)
    yield w
    w.stop()
    # Wait for the worker task to finish (with timeout)
    try:
        await asyncio.wait_for(task, timeout=10.0)
    except (asyncio.TimeoutError, Exception):
        pass
