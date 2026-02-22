# Pikawire event envelope (schema_version 1)

One JSON object per Kafka record. Kafka key = `db:key` (entity-stable:
data_type is deliberately excluded because DEL/`pksetexat`/type-flips carry
unstable types — embedding them would scatter one entity across partitions
and void per-key ordering; the authoritative type is in the record value).

| field | meaning |
|---|---|
| schema_version | contract version (int), breaking changes bump this |
| phase | `snapshot` (full image from the dbsync dump, at the bgsave anchor) / `incremental` (raw binlog entry) / `heartbeat` |
| op | `r` read (full image), `c/u` upsert-ish, `d` delete, `h` heartbeat — a HINT; parse `command` for exact semantics |
| db, data_type, key | logical location of the affected entity |
| command, args | the exact PikiwiDB command as stored in the binlog (args[0] == command) |
| args_encoding | `utf8` (args are verbatim) or `base64` (binary-safe transport) |
| event_id | replay-stable identity (see below) |
| source.id | `host:port` of the source master |
| source.filenum/offset | binlog entry start position; the dump anchor on snapshot events |
| source.seq | monotonically increasing within one process run; breaks ties between events sharing a source position |
| source.exec_time_s | source-side execution second (binlog); emission second for snapshot |

## Consumer guidance

- **State-based sinks (Doris/Postgres/MySQL upsert)**: apply per key in
  `(filenum, offset, seq)` order; unique-key idempotency makes at-least-once
  replay safe. `pksetexat k abs_expiry v` → treat as `SET k v EX (abs_expiry - now)`.
- **Log sinks**: deduplicate on `event_id`.
- `heartbeat` records carry position metadata only; drop unless monitoring.

## Evolution rules

Additive fields only within a major version; consumers must ignore unknown
fields. Breaking changes (semantics of `args`, phase lifecycle, offsets) bump
`schema_version`.
