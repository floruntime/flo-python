# Changelog

All notable changes to the Flo Python SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The op table hash moves with the server in this release: this SDK talks only
to the flo it pins, and the pin lands with the release.
The server's op table hash is now `0x19075df8e1ed5f75` (`TABLE_HASH`).

### Added

- `queue.complete`, `queue.fail` and `queue.touch` (`QUEUE_COMPLETE`,
  `QUEUE_FAIL`, `QUEUE_TOUCH`, 0x182-0x184) report on a dequeue's messages:
  pass the `DequeueResult`, and optionally which of its messages. Each
  returns what applied and the `refused` rest (`QueueRefusal`: seq,
  `QueueRefusalReason`, the server's words); for a complete, `GONE` means
  done, and `QueueReportResult.lost` lists the others
- `DequeueOptions.consumer`, `lease_ms` (5000 to 43200000; the server's 30 s
  when None) and `ack` (complete as taken); `DequeueResult` carries the
  delivery's `term`, `index`, `expires_at_ms` and `consumer`, and each
  `Message` its `deliveries`
- `EnqueueOptions.delay_ms`: how long before the message is ready, 0 to
  86400000 (a day); outside that it raises `ValidationError` before sending
- `queue.purge(queue, PurgeOptions)`: remove every message of a queue,
  ready, leased and dead
- `StorageTier.DISK` (2): a record read back from the durable log. A tier
  this SDK doesn't know reads as a pseudo-member `UNKNOWN_<n>`, not an error
- The ops.json drift test checks the storage tiers too

- `action.info(name)` (`ACTION_INFO`, 0x30F): an action's definition, its
  runs by status and its completed runs' latency, as an `ActionInfo`
- `ActionRunStatus.outcome`: the outcome a completed run's worker named,
  None for success
- `ActionListResult.actions`: each action's version, owner, timeout (its
  lease), attempts and creation time, as `ActionListEntry`; `names` stays
- `StaleReportError`, a `ConflictError`, for reason `STALE_REPORT`: a
  report for a delivery the run has moved on from
- `stream.group_list` and `stream.group_members` (0x15D, 0x15E): a
  stream's groups and a group's members, read on the leader
- `processing.remove` (`PROCESSING_REMOVE`, 0x365): remove a job that no
  longer runs; a running one raises `ConflictError` with
  `Reason.JOB_RUNNING` (31)
- `ProcessingStatusResult.runtime` (this node's records in and out,
  watermark, tick times, uptime) and `.yaml`; `ProcessingListEntry`
  carries `batch_size` and `records_processed`
- `WorkflowDefinitionEntry.enabled`
- `WorkerAwaitOptions.request_key`: an await sent again with the same key
  gets the task the first one took. The action worker keeps its key until an
  await is answered, so a lost answer doesn't strand a task until its lease
  runs out
- `OpCode.TS_INFO` (0x1A7)

- `NoNamespaceError` for status `NO_NAMESPACE` (14), its own error rather
  than a `NotFoundError` (a missing key)
- `WorkflowListRunsOptions.search`: only runs whose id, workflow, step,
  input or tags hold it, ignoring case
- `WorkflowListRunsResult` carries a run list's page (`runs` of
  `WorkflowRunEntry`: `run_id`, `workflow`, `status`, `created_at`) with
  `cursor` and `has_more`
- `Reason.ADMIN_ONLY` (30): the op is served only on a node's admin socket
- `ActionListOptions.cursor` and `ActionListResult.has_more`: `action.list`
  pages
- `stream.delete` (`STREAM_DELETE`, 0x140) deletes a stream's records and
  metadata; a missing stream succeeds, and one that still has records raises
  `ConflictError` unless `StreamDeleteOptions(force=True)`

### Changed

- Queues are at-least-once: a dequeue leases its messages until they're
  completed, failed or expired, and one not completed within its lease is
  delivered again with `deliveries` one higher; five failures or expiries
  make it dead. `QUEUE_DEQUEUE` is 0x181, a typed op
- An enqueue past a queue's caps, or its shard's, raises `OverloadedError`
  (ran `NO`), after the client's usual retries of about 350 ms
- The stream worker retries an unavailable group read (ran `NO`: records
  still being read back from disk, or a leader not ready) with a backoff and
  without logging an error, and its pending drain asks again after an
  unavailable claim instead of giving up until the next reconnect. Neither
  claimed or delivered anything, so the same request is safe to resend
- `retry.is_refused_unrun` excludes a halted shard's refusal (reason
  `SHARD_HALTED`): that one waits for an operator, not a retry
- A stream read's `start` is documented as exclusive (the records after it),
  as the server reads it; the README's stream read examples use the real
  options
- `stale` docs: a shard blocked reading its next entry from disk answers
  reads from before that entry too, as a halted one does
- The README says that while the worker is stopping, a handler's
  cancellation abandons its run

- `queue.dlq_list` sends its page (`DlqListOptions.limit`, 1..100, and
  `after`, the seq that ended the last page) in the request value, and
  returns a `DlqPage`: the dead letters, oldest first, with `total`,
  `has_more` and `last_seq`. It sent nothing, and parsed the answer as a
  dequeue's
- `processing.status` and `processing.list` decode the server's whole
  answer; status refused every answer, which now carries the job's runtime
  and definition after the reason
- `workflow.list_definitions` reads each entry's `enabled` byte; it misread
  every entry after the first
- A handler that raises, `asyncio.CancelledError` included, or runs past
  `action_timeout` fails the run with retry, so its attempts run out; only
  `NonRetryableError` fails it at once (a timeout failed it at once). A task
  is handed back untried (`abandoned=True`) only when the worker is stopping
  or the task running the handler is cancelled from outside. `stop()`
  cancels no handler: they finish and their results are sent
- `auto_touch` touches every sixth of the task's own lease (`lease_ms`), at
  least 100 ms apart, counted from when the task arrived; it touched every
  third of the registered timeout from the handler's start. A hold ends 12 h
  after its delivery whatever the touches
- `ActionRegisterOptions.timeout_ms` defaults to 0, which takes the server's
  300 s lease (it sent 30 s)
- The action worker resends an await refused without an answer (unavailable
  with `ran` unknown, say) with the same `request_key`; a new key follows
  only an answered await
- The KV transaction docs give the server's defaults, 64 ops and 256 KiB,
  both operator settings; a condition, `cas_version` or `incr` inside a
  transaction is decided at commit

- `action.list` returns the page's action names (`ActionListResult.names`)
  and the next page's cursor; it returned an empty list whatever the server
  answered
- A `kv.scan` cursor holds the shard and the last key returned, so it may be
  up to 4098 bytes (was 64); its length prefix is a u16 in the request and
  the answer
- `NAMESPACE_ENSURE` (0x016) is a known op code; shards send it to each
  other, and no client API sends it

### Breaking

- `queue.ack` and `queue.nack` are gone: use `queue.complete` and
  `queue.fail`, which take the `DequeueResult`. `AckOptions` and
  `NackOptions` are gone (`QueueReportOptions`, `QueueFailOptions`), and
  `Message.delivery_count` is now `deliveries`. A consumer that doesn't
  complete its messages sees them again
- `queue.dlq_requeue` and `DlqRequeueOptions` are gone: the server refuses
  every requeue
- `QUEUE_DEQUEUE`, `QUEUE_COMPLETE` and `QUEUE_FAIL` moved to 0x181-0x183
  (0x171, 0x172 and 0x174 are retired), and `OptionTag.DELAY_MS` (0x11) is
  new

- The action ops are the server's typed ops: `ACTION_AWAIT`,
  `ACTION_COMPLETE`, `ACTION_FAIL` and `ACTION_TOUCH` are 0x30A to 0x30D
  (0x306 to 0x309 are retired). A `TaskAssignment` is one delivery of a run:
  `run`, `namespace`, `deliveries`, `term`, `index`, `action`,
  `created_at_ms`, `attempt`, `lease_ms`, `input`, `caller_run`,
  `caller_workflow`, and a `task_id` of `run:deliveries:term:index:action`.
  `worker.complete`, `fail` and `touch` take the task, not an action name and
  task id, and route by its run in its namespace. `touch` takes no extend
  length (the lease is the action's) and returns the new expiry.
  `WorkerFailOptions` gains `abandoned` and `delay_ms`;
  `WorkerCompleteOptions.outcome` defaults to `""`, which is success.
  `WorkerTouchOptions` is gone
- `ActionContext` carries its `task`; `task_id`, `run_id`, `action_name`,
  `payload`, `attempt`, `created_at` and `namespace` read from it, and
  `ctx.touch()` takes no argument
- `ActionRegisterOptions.max_retries` is now `max_attempts`, as the server
  names it; `timeout_ms` is the lease, 5000 to 43200000 ms (0 is the
  server's 300000)
- `queue.peek` calls `QUEUE_PEEK` (0x180; 0x17C is retired, and answered
  empty) with a `limit` of 1..100 (default 20), and returns a `PeekResult`
  of `PeekedMessage` (seq, priority, attempts, enqueued time, size and up to
  4 KiB of payload) with `more`

- Listing workers in a namespace never created raises `NoNamespaceError`, so
  a list right after starting a worker there can raise it until the worker
  has registered (the write that creates the namespace).
- `workflow.history` of a missing run, and a
  `NOT_FOUND` answer to `stream.append`, `read`, `info`, `group_join`,
  `group_leave`, `group_read`, `group_pending` or `group_claim`, raise
  `NotFoundError`; the refusal's body was parsed as the answer
  (`workflow.history` of a missing run returned it as the history)
- The cluster op codes are the server's: `CLUSTER_PROMOTE` (0x032),
  `CLUSTER_REMOVE` (0x033) and `CLUSTER_REPAIR_SKIP` (0x034, admin socket
  only) replace `CLUSTER_JOIN`, `CLUSTER_LEAVE`, `CLUSTER_TRANSFER_LEADER`,
  `CLUSTER_ADD_NODE` and `CLUSTER_REMOVE_NODE`, which the server never
  served. No client API sends them
- `NAMESPACE_CONFIG_SET` and `NAMESPACE_CONFIG_GET` (0x014, 0x015) are gone:
  the server retired them
- `processing.list` returns a `ProcessingListResult` (`jobs`, `cursor`,
  `has_more`) in place of a bare list, so a caller can page past the first
  page with `ProcessingListOptions.cursor`; it dropped the server's cursor
- `workflow.list_definitions` returns a `WorkflowListDefinitionsResult`
  (`definitions` of `WorkflowDefinitionEntry`, `cursor`, `has_more`) in place of
  the raw answer bytes
- `workflow.list_runs` returns a `WorkflowListRunsResult` in place of the raw
  answer bytes, newest first, and refuses a malformed answer.
  `WorkflowListRunsOptions.status_filter` is now `status`. A page can come
  back short with more after it: page until `has_more` is False
- `stream.info` of a stream that doesn't exist raises `NotFoundError`; the
  server answered it with zeros
- A read, list or scan in a namespace that was never created raises
  `NoNamespaceError`; the server answered it as a missing key or an empty
  page. A write still creates the namespace
