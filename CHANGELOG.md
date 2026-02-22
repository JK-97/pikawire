# Changelog

All notable changes to Pikawire are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), versioning is semantic.

## v0.3.0 (2026-02-24)

- Durable snapshot resume (`buffer_fsync: durable`): the dump iterator
  cursor + anchor are persisted in the checkpoint, fsync'd backlog segments
  are retained until the snapshot drain completes, and on restart the stream
  re-attaches at the backlog tail while the dump continues from the cursor
  (dumper ABI v2 `resume_type`/`resume_key`). Unusable coordinates fall back
  to a full re-scan; non-durable behaviour is unchanged.
- Snapshot performance: batched dumper protocol (`snapshot_next_batch`,
  2048-record frames), sidecar output prefetch pipeline, and 4-way parallel
  rsync transfer (1M-key snapshot 12.6s -> 11.5s clean, 52.2s -> 41.5s under
  a 35k ops/s storm).
- Doris pipeline: optional `pipeline.sequence_column` (Doris MoW sequence
  column) stamping load rows with the event position — fixes delete ->
  recreate row loss on Doris 2.1 (`partial_columns` upserts could be
  swallowed after a delete-sign load).
- End-to-end verification against Kafka (Redpanda) and Doris: 50K preload
  + 10K-op net-zero all-type storms, per-key final-state checks with zero
  mismatches (see docs/verification.md).
- Multi-version dump readers in one artifact: `drivers/v35` (pikiwidb
  3.5.6) and `drivers/v40` (4.0.2) each statically link that version's own
  storage layer into a self-contained sidecar dumper, embedded via go:embed
  and exec'd on demand; the dump's engine-directory layout selects the
  driver and every driver fails closed on a foreign layout. Adding a pika
  major = one driver port + one build-script ref.

## v0.2.0 (2026-02-17)

- Declarative row pipeline sinks: key/field mapping rules with same-pk
  coalescing and group-commit upserts for Postgres and MySQL (verified
  live), Doris Stream Load with `partial_columns`, 307 FE→BE redirect
  handling, per-column-shape grouping and delete-sign rows (verified live
  on 2.1).
- Metrics endpoint (`/metrics`, `/healthz`, `/debug/pprof`) with snapshot /
  binlog / delivery gauges.
- Anchor-first handoff: the replication stream attaches at the bgsave
  anchor BEFORE the bulk dump transfer (anchor learned from the small
  `info` file fetched right after DBSync; the master's TryDBSync
  self-validates reusable checkpoints), removing the reposition window.
- Disk-backed handoff backlog: entries arriving during the dump are appended
  to a segmented on-disk log instead of RAM, keeping writer memory bounded
  however long the drain takes (~420 MB under a 6M-key dump at 45k ops/s);
  backpressure is a byte high-watermark + free-disk reserve that blocks
  consume instead of failing the run.
- `snapshot_bgsave: auto|force` (default `auto`): accept a checkpoint the
  master itself deems reusable, escalate to a forced BGSAVE only when its
  anchor was purged.
- Kafka partition key is now `db:key` (one entity, one partition, one
  ordered sub-stream; `data_type` varies per event and stays in the
  payload).
- Strict config parsing with upfront validation; retired keys are rejected
  with actionable messages.

## v0.1.0 (2026-02-10)

- Mode-A event stream: PikiwiDB -> Kafka. PB replication slave
  (MetaSync/TrySync/BinlogSync ack), full snapshot via the master's own
  DBSync RocksDB checkpoint streamed over the rsync service and parsed to
  full-image commands anchored at the exact bgsave binlog position,
  crash-safe delivery checkpointing, heartbeats, replay-stable event ids.
- Debezium-flavored JSON envelope (see docs/format.md); file sink for
  debugging.
- `pikatool` protocol companion (`peek`, `fetchdump`) and the
  `tools/expkit` verification toolchain (independent capture client, stream
  auditor, replayer, full-state comparator).
- Incremental pipeline audited loss-free and in-order against 15.5M binlog
  entries captured by an independent replication client.
