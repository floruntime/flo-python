# Changelog

All notable changes to the Flo Python SDK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The op table hash moves with the server in this release: this SDK talks only
to the flo it pins, and the pin lands with the release.
The server's op table hash is now `0xa8de7758afa69427` (`TABLE_HASH`).

### Added

- `NoNamespaceError` for status `NO_NAMESPACE` (14), its own error rather
  than a `NotFoundError` (a missing key). Reads in a namespace that was never
  created raise it from the server version that adds it; the pinned server
  answers them as a missing key or an empty page
- `Reason.ADMIN_ONLY` (30): the op is served only on a node's admin socket
- `ActionListOptions.cursor` and `ActionListResult.has_more`: `action.list`
  pages
- `stream.delete` (`STREAM_DELETE`, 0x140) deletes a stream's records and
  metadata; a missing stream succeeds, and one that still has records raises
  `ConflictError` unless `StreamDeleteOptions(force=True)`

### Changed

- `action.list` returns the page's action names (`ActionListResult.names`)
  and the next page's cursor; it returned an empty list whatever the server
  answered
- A `kv.scan` cursor holds the shard and the last key returned, so it may be
  up to 4098 bytes (was 64); its length prefix is a u16 in the request and
  the answer
- `NAMESPACE_ENSURE` (0x016) is a known op code; shards send it to each
  other, and no client API sends it

### Breaking

- `workflow.history` and `workflow.status` of a missing run, and a
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
- `ActionListResult.actions` is now `ActionListResult.names`, and
  `ActionInfo` is gone: the action list carries names only
- `processing.list` returns a `ProcessingListResult` (`jobs`, `cursor`,
  `has_more`) in place of a bare list, so a caller can page past the first
  page with `ProcessingListOptions.cursor`; it dropped the server's cursor
- `workflow.list_definitions` returns a `WorkflowListDefinitionsResult`
  (`definitions` of `WorkflowDefinitionEntry`, `cursor`, `has_more`) in place of
  the raw answer bytes
