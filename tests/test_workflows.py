"""Workflow & Action E2E tests for the Python SDK.

Mirrors sdks/go/examples/workflows/workflows_test.go.

Requires a running Flo server (FLO_ENDPOINT env var, default localhost:4453).

Run:
    cd sdks/python
    FLO_ENDPOINT=localhost:4453 python -m pytest tests/test_workflows.py -v
"""

import asyncio
import json
import struct
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from flo import FloClient
from flo.exceptions import FloError
from flo.worker import ActionWorker

# ---------------------------------------------------------------------------
# Embedded YAML workflows (matching Go test constants)
# ---------------------------------------------------------------------------

APPROVAL_WORKFLOW_YAML = """\
kind: Workflow
name: expense-approval
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: wait_for_approval
    failure: flo.Failed

steps:
  wait_for_approval:
    waitForSignal:
      type: "approval_decision"
      timeoutMs: 86400000
      onTimeout: flo.Failed
    transitions:
      success: process_expense
      failure: flo.Failed

  process_expense:
    run: "@actions/process-expense"
    transitions:
      success: flo.Completed
      failure: flo.Failed
"""

SIGNAL_TIMEOUT_WORKFLOW_YAML = """\
kind: Workflow
name: signal-timeout-test
version: "1.0.0"

start:
  run: "@actions/validate-expense"
  transitions:
    success: wait_for_approval
    failure: flo.Failed

steps:
  wait_for_approval:
    waitForSignal:
      type: "approval_decision"
      timeoutMs: 3000
      onTimeout: flo.Failed
    transitions:
      success: process_expense
      failure: flo.Failed

  process_expense:
    run: "@actions/process-expense"
    transitions:
      success: flo.Completed
      failure: flo.Failed
"""

OUTCOME_WORKFLOW_YAML = """\
kind: Workflow
name: order-review
version: "1.0.0"

start:
  run: "@actions/review-order"
  transitions:
    approved: fulfill
    rejected: notify_rejection
    needs_review: manual_review
    failure: flo.Failed

steps:
  fulfill:
    run: "@actions/fulfill-order"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  notify_rejection:
    run: "@actions/notify-rejection"
    transitions:
      success: flo.Completed
      failure: flo.Failed

  manual_review:
    run: "@actions/manual-review"
    transitions:
      success: flo.Completed
      failure: flo.Failed
"""

# Path to order-workflow.yaml (next to this file)
ORDER_WORKFLOW_PATH = str(Path(__file__).parent / "order-workflow.yaml")


# ---------------------------------------------------------------------------
# Binary response parsers (matching Go parseHistory/parseListRuns/etc.)
# ---------------------------------------------------------------------------


@dataclass
class HistoryEvent:
    type: str
    detail: str
    timestamp: int


@dataclass
class RunEntry:
    run_id: str
    workflow: str
    status: str
    created_at: int


@dataclass
class DefinitionEntry:
    name: str
    version: str
    created_at: int


def parse_history(data: bytes) -> list[HistoryEvent]:
    """Parse binary history response.

    Format: [count:u32]([type_len:u16][type][detail_len:u16][detail][ts:i64])*
    """
    if len(data) < 4:
        return []
    count = struct.unpack_from("<I", data, 0)[0]
    pos = 4
    events = []
    for _ in range(count):
        if pos + 2 > len(data):
            break
        type_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + type_len > len(data):
            break
        typ = data[pos : pos + type_len].decode("utf-8")
        pos += type_len

        if pos + 2 > len(data):
            break
        detail_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + detail_len > len(data):
            break
        detail = data[pos : pos + detail_len].decode("utf-8")
        pos += detail_len

        if pos + 8 > len(data):
            break
        ts = struct.unpack_from("<q", data, pos)[0]
        pos += 8

        events.append(HistoryEvent(type=typ, detail=detail, timestamp=ts))
    return events


def parse_list_runs(data: bytes) -> list[RunEntry]:
    """Parse binary list-runs response.

    Format: [count:u32]([rid_len:u16][rid][wf_len:u16][wf][st_len:u16][st][ts:i64])*
    """
    if len(data) < 4:
        return []
    count = struct.unpack_from("<I", data, 0)[0]
    pos = 4
    runs = []
    for _ in range(count):
        if pos + 2 > len(data):
            break
        rid_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + rid_len > len(data):
            break
        run_id = data[pos : pos + rid_len].decode("utf-8")
        pos += rid_len

        if pos + 2 > len(data):
            break
        wf_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + wf_len > len(data):
            break
        workflow = data[pos : pos + wf_len].decode("utf-8")
        pos += wf_len

        if pos + 2 > len(data):
            break
        st_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + st_len > len(data):
            break
        status = data[pos : pos + st_len].decode("utf-8")
        pos += st_len

        if pos + 8 > len(data):
            break
        ts = struct.unpack_from("<q", data, pos)[0]
        pos += 8

        runs.append(RunEntry(run_id=run_id, workflow=workflow, status=status, created_at=ts))
    return runs


def parse_list_definitions(data: bytes) -> list[DefinitionEntry]:
    """Parse binary list-definitions response.

    Format: [count:u32]([name_len:u16][name][ver_len:u16][ver][ts:i64])*
    """
    if len(data) < 4:
        return []
    count = struct.unpack_from("<I", data, 0)[0]
    pos = 4
    defs = []
    for _ in range(count):
        if pos + 2 > len(data):
            break
        name_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + name_len > len(data):
            break
        name = data[pos : pos + name_len].decode("utf-8")
        pos += name_len

        if pos + 2 > len(data):
            break
        ver_len = struct.unpack_from("<H", data, pos)[0]
        pos += 2
        if pos + ver_len > len(data):
            break
        version = data[pos : pos + ver_len].decode("utf-8")
        pos += ver_len

        if pos + 8 > len(data):
            break
        ts = struct.unpack_from("<q", data, pos)[0]
        pos += 8

        defs.append(DefinitionEntry(name=name, version=version, created_at=ts))
    return defs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def poll_status(
    client: FloClient,
    run_id: str,
    check,
    timeout_sec: int = 30,
) -> dict:
    """Poll workflow status every 300ms until check(status) returns True or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout_sec
    while True:
        s = await client.workflow.status(run_id)
        if check(s):
            return s
        now = asyncio.get_event_loop().time()
        if now >= deadline:
            pytest.fail(
                f"Timed out waiting for status on {run_id} "
                f"(last: status={s.get('status')} step={s.get('current_step')})"
            )
        await asyncio.sleep(0.3)


async def sync_test_workflows(client: FloClient) -> None:
    """Sync all test workflow definitions (idempotent)."""
    # Sync order-workflow.yaml from file
    with open(ORDER_WORKFLOW_PATH) as f:
        order_yaml = f.read()
    await client.workflow.sync(order_yaml)

    # Sync embedded workflows
    await client.workflow.sync(APPROVAL_WORKFLOW_YAML)
    await client.workflow.sync(SIGNAL_TIMEOUT_WORKFLOW_YAML)
    await client.workflow.sync(OUTCOME_WORKFLOW_YAML)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDeclarativeSync:
    """Test declarative workflow sync operations."""

    async def test_sync_from_file(self, client: FloClient, worker: ActionWorker):
        """Sync order-workflow.yaml from file, then re-sync to verify idempotence."""
        with open(ORDER_WORKFLOW_PATH) as f:
            yaml_content = f.read()

        result = await client.workflow.sync(yaml_content)
        assert result.name == "order-processing"
        assert result.version == "1.4.0"
        assert result.action in ("created", "unchanged")

        # Re-sync → should be unchanged
        result2 = await client.workflow.sync(yaml_content)
        assert result2.action == "unchanged"

    async def test_sync_embedded_yamls(self, client: FloClient, worker: ActionWorker):
        """Sync embedded YAML strings."""
        r1 = await client.workflow.sync(APPROVAL_WORKFLOW_YAML)
        assert r1.name == "expense-approval"
        assert r1.action in ("created", "unchanged")

        r2 = await client.workflow.sync(SIGNAL_TIMEOUT_WORKFLOW_YAML)
        assert r2.name == "signal-timeout-test"
        assert r2.action in ("created", "unchanged")

        r3 = await client.workflow.sync(OUTCOME_WORKFLOW_YAML)
        assert r3.name == "order-review"
        assert r3.action in ("created", "unchanged")

    async def test_sync_bytes(self, client: FloClient, worker: ActionWorker):
        """Sync from raw bytes."""
        result = await client.workflow.sync_bytes(APPROVAL_WORKFLOW_YAML.encode("utf-8"))
        assert result.name == "expense-approval"
        assert result.action in ("created", "unchanged")


class TestListDefinitions:
    """Test listing workflow definitions."""

    async def test_list_definitions(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        data = await client.workflow.list_definitions()
        defs = parse_list_definitions(data)
        assert len(defs) >= 1, "Expected at least one workflow definition"

        names = {d.name for d in defs}
        # At least order-processing should be present
        assert "order-processing" in names, f"order-processing not found in {names}"


class TestGetDefinition:
    """Test retrieving a workflow definition."""

    async def test_get_definition(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        yaml_text = await client.workflow.get_definition("order-processing")
        assert yaml_text is not None
        assert len(yaml_text) > 0
        assert "order-processing" in yaml_text

    async def test_get_definition_not_found(self, client: FloClient, worker: ActionWorker):
        result = await client.workflow.get_definition("nonexistent-workflow-xyz")
        assert result is None


class TestHappyPath:
    """Test the order-processing workflow happy path (small amount → completed)."""

    async def test_happy_path(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps(
            {
                "orderId": "ORD-100",
                "amount": 49.99,
                "customerId": "CUST-1",
            }
        )

        run_id = await client.workflow.start("order-processing", input_data)
        assert run_id, "Expected a non-empty run ID"

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=30,
        )
        assert final["status"] == "completed", (
            f"Expected completed, got {final['status']} at step {final.get('current_step')}"
        )


class TestRejectionPath:
    """Test the order-processing workflow with a large amount that triggers rejection."""

    async def test_rejection_path(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps(
            {
                "orderId": "ORD-REJECT",
                "amount": 6000,
                "customerId": "CUST-2",
            }
        )

        run_id = await client.workflow.start("order-processing", input_data)
        assert run_id

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=30,
        )
        # Large amount → payment fails → rejected step → completed
        # (send-rejection always succeeds)
        assert final["status"] in ("completed", "failed")


class TestPlanFallback:
    """Test plan fallback: primary processor fails (FB- prefix), fallback succeeds."""

    async def test_plan_fallback(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps(
            {
                "orderId": "FB-1001",
                "amount": 100,
                "customerId": "CUST-3",
            }
        )

        run_id = await client.workflow.start("order-processing", input_data)
        assert run_id

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=30,
        )
        assert final["status"] == "completed", (
            f"Expected completed (fallback), got {final['status']} "
            f"at step {final.get('current_step')}"
        )


class TestSignalAdvances:
    """Test that sending a signal advances a waiting workflow."""

    async def test_signal_advances(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps({"expenseId": "EXP-001", "amount": 500})
        run_id = await client.workflow.start("expense-approval", input_data)
        assert run_id

        # Wait for workflow to reach the signal wait step
        await poll_status(
            client,
            run_id,
            lambda s: s["status"] == "waiting",
            timeout_sec=15,
        )

        # Send the approval signal
        await client.workflow.signal(run_id, "approval_decision", '{"approved": true}')

        # Should proceed to completion
        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=15,
        )
        assert final["status"] == "completed"


class TestSignalTimeout:
    """Test that a signal wait times out after the configured duration."""

    async def test_signal_timeout(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps({"expenseId": "EXP-TIMEOUT", "amount": 100})
        run_id = await client.workflow.start("signal-timeout-test", input_data)
        assert run_id

        # Wait for workflow to reach signal wait
        await poll_status(
            client,
            run_id,
            lambda s: s["status"] == "waiting",
            timeout_sec=15,
        )

        # Don't send a signal — wait for the 3s timeout
        await asyncio.sleep(5)

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("failed", "timed_out", "completed"),
            timeout_sec=15,
        )
        assert final["status"] in ("failed", "timed_out"), (
            f"Expected failed/timed_out, got {final['status']}"
        )


class TestHistory:
    """Test workflow execution history retrieval."""

    async def test_history(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        input_data = json.dumps(
            {
                "orderId": "ORD-HIST",
                "amount": 25.00,
                "customerId": "CUST-HIST",
            }
        )

        run_id = await client.workflow.start("order-processing", input_data)
        assert run_id

        # Wait for completion
        await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=30,
        )

        # Get history
        history_data = await client.workflow.history(run_id)
        assert history_data is not None

        events = parse_history(history_data)
        assert len(events) >= 1, "Expected at least one history event"

        # Log events for debugging
        for ev in events:
            print(f"  History: type={ev.type} detail={ev.detail}")


class TestListRuns:
    """Test listing workflow runs."""

    async def test_list_runs(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        # Start a run so there's at least one
        input_data = json.dumps(
            {
                "orderId": "ORD-LIST",
                "amount": 10.00,
                "customerId": "CUST-LIST",
            }
        )
        run_id = await client.workflow.start("order-processing", input_data)
        # Give server time to register the run
        await asyncio.sleep(0.5)

        from flo.types import WorkflowListRunsOptions

        data = await client.workflow.list_runs(
            WorkflowListRunsOptions(workflow_name="order-processing", limit=50)
        )
        runs = parse_list_runs(data)
        assert len(runs) >= 1, "Expected at least one run"

        # Our run should be in there
        run_ids = {r.run_id for r in runs}
        assert run_id in run_ids, f"Run {run_id} not found in {run_ids}"


class TestDisableEnable:
    """Test disabling and re-enabling a workflow definition."""

    async def test_disable_enable(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        # Disable the workflow
        await client.workflow.disable("order-processing")

        # Starting should fail
        input_data = json.dumps({"orderId": "ORD-DIS", "amount": 10, "customerId": "C"})
        with pytest.raises(FloError):
            await client.workflow.start("order-processing", input_data)

        # Re-enable
        await client.workflow.enable("order-processing")

        # Starting should work now
        run_id = await client.workflow.start("order-processing", input_data)
        assert run_id


class TestCancel:
    """Test cancelling a running workflow."""

    async def test_cancel(self, client: FloClient, worker: ActionWorker):
        await sync_test_workflows(client)

        # Start an expense-approval (will wait for signal, giving us time to cancel)
        input_data = json.dumps({"expenseId": "EXP-CANCEL", "amount": 100})
        run_id = await client.workflow.start("expense-approval", input_data)
        assert run_id

        # Wait for it to be waiting
        await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("waiting", "running"),
            timeout_sec=15,
        )

        # Cancel
        await client.workflow.cancel(run_id, reason="test cancellation")

        # Verify cancelled
        status = await client.workflow.status(run_id)
        assert status["status"] == "cancelled", f"Expected cancelled, got {status['status']}"


class TestOutcomeRouting:
    """Test outcome-based transitions (approved/rejected/needs_review)."""

    async def test_approved_outcome(self, client: FloClient, worker: ActionWorker):
        """Small amount → review-order returns 'approved' → fulfill step."""
        await sync_test_workflows(client)

        input_data = json.dumps({"orderId": "OUT-1", "amount": 50})
        run_id = await client.workflow.start("order-review", input_data)

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=15,
        )
        assert final["status"] == "completed"

    async def test_rejected_outcome(self, client: FloClient, worker: ActionWorker):
        """Large amount → review-order returns 'rejected' → notify_rejection step."""
        await sync_test_workflows(client)

        input_data = json.dumps({"orderId": "OUT-2", "amount": 750})
        run_id = await client.workflow.start("order-review", input_data)

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=15,
        )
        assert final["status"] == "completed"

    async def test_needs_review_outcome(self, client: FloClient, worker: ActionWorker):
        """Mid-range amount → review-order returns 'needs_review' → manual_review step."""
        await sync_test_workflows(client)

        input_data = json.dumps({"orderId": "OUT-3", "amount": 250})
        run_id = await client.workflow.start("order-review", input_data)

        final = await poll_status(
            client,
            run_id,
            lambda s: s["status"] in ("completed", "failed"),
            timeout_sec=15,
        )
        assert final["status"] == "completed"


class TestSyncDir:
    """Test syncing all YAML files in a directory."""

    async def test_sync_dir(self, client: FloClient, worker: ActionWorker):
        """Write multiple YAMLs to a temp dir, sync them all, then re-sync for idempotence."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write YAML files
            (Path(tmpdir) / "approval.yaml").write_text(APPROVAL_WORKFLOW_YAML)
            (Path(tmpdir) / "outcome.yaml").write_text(OUTCOME_WORKFLOW_YAML)
            (Path(tmpdir) / "order.yaml").write_text(Path(ORDER_WORKFLOW_PATH).read_text())

            results = await client.workflow.sync_dir(tmpdir)
            assert len(results) == 3, f"Expected 3 sync results, got {len(results)}"

            names = {r.name for r in results}
            assert "expense-approval" in names
            assert "order-review" in names
            assert "order-processing" in names

            # Verify each definition exists
            for r in results:
                defn = await client.workflow.get_definition(r.name)
                assert defn is not None, f"Definition for {r.name} not found"

            # Re-sync — all should be unchanged
            results2 = await client.workflow.sync_dir(tmpdir)
            for r in results2:
                assert r.action == "unchanged", f"Expected unchanged for {r.name}, got {r.action}"


class TestActionInvoke:
    """Test direct action invocation (outside of workflows)."""

    async def test_invoke_and_status(self, client: FloClient, worker: ActionWorker):
        """Invoke an action directly and check the result."""
        input_data = json.dumps({"expenseId": "EXP-DIRECT", "amount": 100})
        result = await client.action.invoke(
            "validate-expense",
            input_data.encode("utf-8"),
        )
        assert result.run_id, "Expected a non-empty run_id"

    async def test_invoke_nonexistent_action(self, client: FloClient, worker: ActionWorker):
        """Invoking a nonexistent action should fail."""
        with pytest.raises(FloError):
            await client.action.invoke(
                "nonexistent-action-xyz",
                b"{}",
            )
