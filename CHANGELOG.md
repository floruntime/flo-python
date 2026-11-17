# Changelog

All notable changes to the Flo Python SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The server's op table hash is now `0xac07f1f7338c66bd` (`TABLE_HASH`).

### Added

- `stream.group_steal(stream, group, consumer, records, min_idle_ms)`: take
  records another consumer holds and has left idle that long, each with a new
  delivery and lease, its failures kept. It answers a `GroupStealResult`, a
  delivery acked, nacked or touched as a read's, with `refused` for the
  records it didn't take (`NOT_IDLE` for one its holder touched within the
  idle time)
- A consumer group's dead letters: `stream.group_dlq_list` (a page, oldest
  dead first, with each one's deliveries, failures, `dead_at_ms` and last
  error; page by the opaque `cursor`), `group_dlq_requeue` (ready again,
  failures reset) and `group_dlq_delete` (with `dry_run`). Requeue and delete
  name up to 100 records by id, or take the oldest with `all`, a batch of up to
  `limit`; they answer `applied`, `refused` (with why and the server's words)
  and `more`
- `queue.dlq_requeue` and `queue.dlq_delete` (with `dry_run`), by seq or
  `all`, answering a `DlqResult` as the group's do
- `QueueRefusalReason.NOT_DEAD` (6) and `NOT_IDLE` (7): a requeue or delete
  of one that isn't dead, and a steal of one that isn't idle.
  `GroupRefusal.reason` carries the server's words where it gives them
- `ActionRunStatus.held`: why a run's lease, past due, isn't ended yet (its
  end waits for room on the calling workflow's shard)
- `stream.create` (partitions, retention by age and count) and `stream.alter`
  (retention); `StreamInfo.retention_age_s` and `retention_count`

### Changed

- A worker's complete, fail, ack or nack refused without being run
  (overloaded or unavailable, after the client's own retries) is sent again
  with a capped backoff until the server takes it or answers otherwise, and
  logged; it used to be dropped. A complete or fail that ends a run a
  workflow step called is refused overloaded while the server's writes to the
  workflow's shard are queued full, and the run's lease doesn't run out
  meanwhile. A stopping worker gives up after a couple of seconds
- An alter of a stream an append made succeeds; it used to be not found.
  After an alter a create must name the stream's settings as altered, or it
  raises `ConflictError`; a create whose name hashes as another stream's does
  too
- A workflow step whose action isn't registered waits for one, up to the
  server's missing-action wait, instead of failing at once

### Breaking

- `queue.dlq_list` sends the new op: `DlqPage` is `messages` and `cursor`
  (a `DlqCursor`, `None` on the last page), replacing `total`, `has_more` and
  `last_seq`; `DlqListOptions.after` takes the cursor, and `limit` defaults
  to 20. `DeadLetter` gains `failures`, `dead_at_ms` and `error`, and loses
  `moved_at_ms`. A queue that doesn't exist raises `NotFoundError`
- The old dead letter op codes (0x176..0x178) are gone; `OpCode.QUEUE_DLQ_*`
  are the server's 0x185..0x187

## [0.1.6] - 2026-11-16

The op table hash moves with the server in this release: this SDK talks only
to the flo it pins, and the pin lands with the release.
The server's op table hash is now `0x17f3632347cd6dd9` (`TABLE_HASH`).

### Added

- `client.ts`: `write`, `read` (one page), `iter_points` (every page) and
  `floql`. A read takes `field`, `tags`, `from_ns`/`to_ns` (inclusive),
  `limit` (1..4096) and `order` (`TSOrder.OLDEST_FIRST` or `NEWEST_FIRST`),
  and answers the points of every matching series merged by time, then
  write order, with an opaque cursor that is `None` on the last page. A
  page goes on strictly past the cursor, so no point is read twice or
  skipped; page until the cursor is `None`, not until a page is short
- Times are Unix ns `int`s: `TSWriteOptions.time_ns` (`OptionTag.TS_TIMESTAMP_NS`,
  0x67, an i64; none or 0 is the server's time), `TSWriteResult.time_ns`,
  `TSPoint.time_ns`, `FloQLPoint.time_ns`. `ns_from_datetime` and
  `datetime_from_ns` (and `TSPoint.time`) convert at the microsecond
- `ts.query` (`TSQueryOptions`: `from_ns`, `to_ns`, a `window` as ns or a
  `timedelta`, and `TSAggregation`) answers `TSBucket`s with `start_ns`;
  `ts.info` answers a `TSInfo` with `oldest_ns` and `newest_ns`.
  `ns_from_timedelta` converts exactly
- `ProcessingRuntime.late_edge_ms` and `late_points`: at least that many of
  a job's TS source points came in later than the edge at or above its
  lateness window, so its sources never read them; both `None` for a job
  with no TS source
- `ActionWorker.close(timeout=30.0)` and `StreamWorker.close(timeout=30.0)`
  wait up to `timeout` for `start()` to return, so in-flight completes,
  fails, acks, nacks and hand-backs are sent before the connections close

### Changed

- `stream.read` of a stream that doesn't exist (never created, or deleted)
  raises `NotFoundError`, as the server now answers; an existing stream
  with nothing to read is still empty, and a blocking read of a stream not
  created yet still waits for its first append. `ts.read` and `iter_points`
  raise `NotFoundError` when no series of the field matches. A consumer
  that may start before its stream or series exists catches it and starts
  again. A not found answered stale (by a node that may be behind) is
  `StaleNotFoundError`, as for `kv.get`: the stream or series may exist, so
  read again
- A stream worker whose read is answered not found fails `start()` with
  `NotFoundError` instead of retrying as if the stream were empty
- A worker task cancelled while its handler runs waits (up to the handler
  timeout) for the handler to end before handing the work back; a cancel
  aimed at the task while a timed-out handler ends is no longer swallowed
- A FloQL answer of more than 4096 raw points is refused by the server
  (`BadRequestError`, with its text); aggregate, narrow, or page with
  `ts.read`

### Breaking

- `OptionTag.TS_TIMESTAMP` is `TS_TIMESTAMP_NS`: the server reads it as ns,
  so a time sent as ms lands near 1970. A time before 1970 is refused
- `OptionTag.TS_FROM_MS`, `TS_TO_MS` and `TS_WINDOW_MS` are `TS_FROM_NS`,
  `TS_TO_NS` and `TS_WINDOW_NS`, i64s of ns. Every time-series time and
  window is ns; no ms remains in the time-series API

## [0.1.5] - 2026-11-15

The op table hash moves with the server in this release: this SDK talks only
to the flo it pins, and the pin lands with the release.
The server's op table hash is now `0xc1f93eb0ed86a281` (`TABLE_HASH`).

### Added

- `Reason.QUEUE_FULL` (36) and `GROUP_HELD_FULL` (37), both under status
  overloaded. `QueueFullError` (an `OverloadedError`) is retried like any
  overloaded refusal that didn't run: a full queue drains as messages are
  completed. `GroupHeldFullError` (an `OverloadedError`) is never retried: a
  group read takes nothing new until the group (whose dead records count, and
  stay until it is deleted or the stream trimmed) or its shard has room. The
  stream worker logs it once and backs off, as for a limit;
  `retry.is_group_held_full` tells it apart
- `StreamGroupNackOptions.abandon` (`OptionTag.ABANDON`, 0x34) hands records
  back unstarted: ready at once, the delivery counted, no failure. It takes
  no `dead`, `delay_ms` or `error`, and never dead-letters
- A refusal reason or `ran` this SDK doesn't know decodes as `UNKNOWN_<n>`,
  keeping the number and the server's message, instead of raising
  `IncompleteResponseError`
- Consumer groups lease each record. `stream.group_read` returns a
  `GroupReadResult` (the delivery's `term`, `index`, `expires_at_ms` and
  `consumer`) of `GroupRecord`s, each with its `deliveries`.
  `stream.group_ack`, `group_nack` and the new `group_touch` take that result
  and optionally which of its records, and return what applied and the
  `refused` rest (`GroupRefusal`: record id and `QueueRefusalReason`); a
  stale report applies nothing to its records and still succeeds.
  `StreamGroupNackOptions` gains `delay_ms`, `dead` and `error`
- `stream.group_create` (with `StreamGroupCreateOptions`: `ack_timeout_ms`,
  5 s to 12 h, and `max_deliver`, 1..255), `group_delete` and `group_info`
  (`StreamGroupInfo`: ready, leased, dead, holders, trimmed_pending)
- `StreamWorker` options `ack_timeout_ms` and `max_deliver` (the worker
  creates the group with them at start) and `auto_touch` (touch a read's
  unacked records every sixth of the lease from its arrival, those waiting
  for a handler slot included; transient refusals are tried again).
  `StreamContext.deliveries`
- `LimitError` for the new status `LIMIT` (15), with reasons
  `MEMORY_BUDGET` (32), `TOO_MANY_ACTIONS` (33),
  `TOO_MANY_WORKFLOW_DEFINITIONS` (34) and `TOO_MANY_WORKFLOW_RUNS` (35).
  It is never retried. For a worker, start() fails with the limit error; a
  limit hit while running is logged and backed off, once per reason
- `OptionTag.DEAD` (0x32)

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
  stream's groups and the consumers holding a group's records
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

- A group nack carries its error in the body, at most 1024 bytes (was an
  option of at most 255); over 1024 raises `ValidationError` before sending
- An ack, nack or touch to a group that isn't there raises `NotFoundError`,
  not a result with every record `GONE`
- The stream worker hands back the records of its last batch it hasn't
  started with abandon, on stop and when its task is cancelled, and so a
  record whose handler is cancelled while the worker stops: they spend no
  attempt (a plain nack spent one). A handler that raises while the worker
  isn't stopping still nacks
- `LimitError` and the README say the limits are fixed in the server's
  build, not raised by a setting; `TOO_MANY_ACTIONS` counts per shard
- Queues are at-least-once: a dequeue leases its messages until they're
  completed, failed or expired, and one not completed within its lease is
  delivered again with `deliveries` one higher; five failures or expiries
  make it dead. `QUEUE_DEQUEUE` is 0x181, a typed op
- An enqueue past a queue's caps, or its shard's, raises `OverloadedError`
  (ran `NO`), after the client's usual retries of about 350 ms
- The stream worker retries an unavailable group read (ran `NO`: records
  still being read back from disk, or a leader not ready) with a backoff and
  without logging an error. It leased nothing, so the same read is safe to
  resend
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

- `PeekedMessage.attempts` and `DeadLetter.attempts` are now `deliveries`, as
  the server names them
- `OptionTag.ERROR_TEXT` (0x33) is gone: a nack's error is in its body
- `stream.group_join`, `group_leave` and `group_claim` are gone, with
  `StreamGroupJoinOptions`, `StreamClaimResult` and the op codes for join,
  leave, claim and configure_sweeper: the server refuses them. A group read
  takes expired and nacked records before new ones. `group_ack` and
  `group_nack` take the read's `GroupReadResult`, not record ids, and
  `StreamGroupAckOptions`/`StreamGroupNackOptions` lose `consumer` (the read
  names it). The stream worker no longer joins, leaves or drains its pending
  records on reconnect: `redeliver_pending_on_reconnect` and
  `claim_min_idle_ms` are gone; held records come back when their lease runs
  out. `group_pending` takes `namespace` in place of options
- KV `routing_key` is gone from every option type (`GetOptions`,
  `PutOptions`, `DeleteOptions`, `HistoryOptions`, `KVIncrOptions`,
  `KVExpireOptions`, `KVExistsOptions`, `KVJsonOptions`, `KVMGetOptions`): a
  key routes by its `{tag}`, or the whole key when it has none.
  `kv.begin(key)` pins the transaction to the key's `{tag}`; a key that
  routes elsewhere is refused with `Reason.KEYS_NOT_COLOCATED` (18, was
  `WRONG_ROUTING_KEY`). `mget` gathers keys across shards on the server
- The largest KV value is 257,916 bytes and the largest JSON document
  256,918

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
  of `PeekedMessage` (seq, priority, deliveries, enqueued time, size and up to
  4 KiB of payload) with `more`

- Listing workers in a namespace never created raises `NoNamespaceError`, so
  a list right after starting a worker there can raise it until the worker
  has registered (the write that creates the namespace).
- `workflow.history` of a missing run, and a
  `NOT_FOUND` answer to `stream.append`, `read`, `info`, `group_read` or
  `group_pending`, raise
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
