# Pikawire

**Change Data Capture for [PikiwiDB](https://github.com/OpenAtomFoundation/pikiwidb) (Pika).**
Stream every key — full snapshot + live changes — into Kafka as a faithful, replayable
raw-command event stream.

```
PikiwiDB ──(slave protocol: DBSync dump + PB binlog replication)──▶ pikawire ──▶ Kafka ──▶ {Doris, Flink, Spark, ES, …}
```

## Why

PikiwiDB has no first-class CDC tooling. Pikawire treats it like MySQL treats
Debezium/Flink-CDC: a standard, self-describing **mode-A raw event stream** —
the tool faithfully transports commands, downstream consumers own the
modeling. That boundary is exactly why Debezium won.

## Highlights

- **Full + incremental, exact anchor, no manual handoff.** The snapshot is
  PikiwiDB's own RocksDB checkpoint (master `bgsave`), streamed over the
  rsync service and parsed to full-image commands. It is a point-in-time
  consistent view anchored at the exact bgsave binlog position — the
  incremental stream resumes from that anchor with zero reconciliation
  window. See [docs/design.md](docs/design.md).
- **No data loss across crashes.** Delivery checkpointing is anchored to the
  binlog position; gaps written while the tool is down are replayed on
  restart.
- **Standard wire format.** Debezium-style envelope, entity-partitioned Kafka
  keys, heartbeats, replay-stable event ids.

## Build

Full sync requires the dbsync dump reader, embedded as **per-pika-version
driver plugins** in one artifact:

```bash
bash scripts/build-cgo.sh
# -> bin/pikawire-dbsync  (single binary serving pika 3.5.x AND 4.0.x masters)
```

Each driver (`drivers/v35`, `drivers/v40`) statically links that pikiwidb
version's OWN storage layer (`libstorage.a`/`libpstd.a`) into a self-contained
**sidecar dumper**, embedded via go:embed and exec'd on demand. Sidecars (not
dlopen) because the storage layer's TLS image exceeds glibc's static-TLS
budget for runtime-loaded objects — and a storage-layer crash cannot take the
product with it. The dump's engine-directory naming (`strings/...` vs
`0/1/2`) picks the driver; a mismatched driver reports a clean error. Adding
a future pika major = new `drivers/<name>` port + one ref in the build script.

First build per supported version clones pikiwidb and builds its deps
(~15–25 min, cached under `$HOME/.cache/pikawire-pikiwidb`); afterwards the
build is fast. The Go side needs no pika toolchain at all — only the
generated dumper artifacts.

> Upgrade-trap note (measured): pika 4.0.x opening a 3.5.x data directory
> does NOT migrate or fail — it starts an empty database beside the old
> files. Always verify the layout before switching a running master's image.

Builds without the `pikadump` tag still compile (useful for CI and the
`pikatool` protocol utilities) but refuse to run `pikawire` at config
validation: without the dump readers there is no way to produce a
point-in-time consistent snapshot, and a binary that silently degrades to
an incomplete guarantee is worse than one that fails loudly.

## Quickstart

```bash
docker run --rm ghcr.io/jk-97/pikawire:pikawire -c /etc/pikawire/config.yaml
```

```yaml
source:   { host: 127.0.0.1, port: 9221, db: db0 }
sink:
  kafka:  { brokers: ["kafka:9092"], topic: pikawire.db0 }
checkpoint: pikawire.checkpoint.json
dump_root: pikawire-dump          # working dir for the fetched dbsync dump
# snapshot_bgsave: auto         # auto = reuse the master's valid checkpoint (default); force = demand a fresh BGSAVE
# buffer_dir: pikawire-buffer     # disk-backed backlog for binlog arriving during the dump
# pending_bytes_high: 85899345920   # backpressure watermark (block, never fail)
# buffer_fsync: none            # none|durable; durable additionally fsyncs
#                               the backlog and enables crash-resume of an
#                               interrupted snapshot (no full re-scan)
include_ttl: true
heartbeat: 30s
```

The master must allow `bgsave` and expose the rsync transfer port (default
`21049` alongside the Redis port). Inspect the event stream without any sink:
`pikatool peek` prints decoded replication entries.

## Event format (contract)

One JSON object per Kafka record (debezium-flavored):

```json
{
  "schema_version": 1,
  "phase": "incremental",           // snapshot | incremental | heartbeat
  "op": "d",                         // r | c | u | d | h
  "db": "db0", "data_type": "hash",
  "key": "user:1", "command": "HSET",
  "args": ["HSET", "user:1", "name", "tom"], "args_encoding": "utf8",
  "event_id": "inc:10.0.0.1:9221:0:8749:42",
  "source": { "id": "10.0.0.1:9221", "db": "db0", "filenum": 0, "offset": 8749, "seq": 42, "exec_time_s": 1788461076 }
}
```

Kafka message key: `db:key` — one entity, one partition, one ordered
sub-stream (`data_type` varies per event and must not steer routing; it stays
in the payload). Full details: [docs/format.md](docs/format.md).

## Semantics

- At-least-once delivery; consumers deduplicate by `event_id` (stable across
  replays). Order per key by `(source.filenum, source.offset, source.seq)` —
  snapshot events carry a record ordinal in `seq` because several commands can
  share the dump anchor; incremental `seq` is 0 (its position is unique).
  Snapshot events are full images and replay-safe; incremental events
  preserve binlog order per key.
- Known limitation: PikiwiDB TTL expiry emits **no binlog event** (verified on
  3.5.6 and 4.0.2). `include_ttl: true` carries TTLs in `PEXPIRE` companion
  events; the dbsync dump anchors at bgsave position so keys expired *before*
  the checkpoint are simply absent — but keys expiring *after* it stay in the
  stream. Strict delete convergence needs key-versioning or scheduled
  full-key reconciliation downstream.

## Benchmarks

Single Linux test host, Dockerized PikiwiDB 3.5.6, mixed 5-type universe
(~330 B/event), `file`/Kafka sink at steady state:

| scenario | result |
|----------|--------|
| 1M-key full snapshot, no interference | **11.5 s** end to end (~87k rec/s), dump transfer < 1 s |
| 1M-key snapshot + concurrent ~35k ops/s write storm | **41.5 s** to snapshot-complete (includes draining the whole backlog in order) |
| 50K-key snapshot into Kafka (Redpanda) | **714 ms** emission (~70k events/s); start -> caught-up in 67.9 s with a 10K-op storm running throughout |
| crash-resume (durable mode) | killed mid-dump -> resumes from cursor + fsync'd backlog, **zero re-fetch**, +<1 s overhead vs non-durable |
| memory | ~50 MB RSS steady state; ~420 MB during a 6M-key dump under 45k ops/s (disk backlog, bounded) |
| end-to-end lag (idle tail) | < 1 s behind the master at ~35k ops/s ingestion |

Protocol: the snapshot path is IO-bound on the master's checkpoint transfer
and the RocksDB scan; the numbers above measure emit->sink with everything
local. Methodology and how to re-measure: [docs/verification.md](docs/verification.md).

## Row pipeline into Doris (no Kafka)

Pikawire can also map keys declaratively onto relational rows. For Doris
(create the tables yourself — unique-key, merge-on-write, plus a sequence
column so delete->recreate ordering survives concurrent loads):

```sql
CREATE TABLE pikawire.t_user (
  uid  VARCHAR(32),
  name VARCHAR(255),
  age  BIGINT,
  seqv BIGINT
) UNIQUE KEY(uid)
DISTRIBUTED BY HASH(uid) BUCKETS 4
PROPERTIES ("replication_num" = "1", "function_column.sequence_type" = "bigint");
```

```yaml
pipeline:
  driver: doris
  fenodes: ["127.0.0.1:8030"]
  user: root
  password: ""
  sequence_column: seqv      # event position stamped per load row (MoW ordering)
  rules:
    - types: [hash]
      key_pattern: "user:{uid}"     # {capture} -> column value
      table: pikawire.t_user
      pk: [uid]
      fields: { name: name, age: age }
```

Deletes arrive as `__DORIS_DELETE_SIGN__` loads; keys matching no rule are
skipped; delivery is at-least-once and idempotent (unique-key upsert).
Same engine drives Postgres and MySQL with `driver: postgres|mysql` + a DSN
— full mapping semantics: [docs/pipeline.md](docs/pipeline.md), runnable
config in [examples/pipeline-doris.yaml](examples/pipeline-doris.yaml).

## Verified

The PB replication findings (handshake, offset spaces, keepalive, purge and
checkpoint-reuse semantics) are validated against 3.5.6 and 4.0.2; the
stream is audited loss-free and in-order against independently captured
binlog ground truth, sinks are checked by full per-key state diffs, and
crash windows (including durable snapshot resume) are exercised — see
[docs/verification.md](docs/verification.md) for the property matrix,
tools and representative results.

## Configuration

A full reference lives in `examples/`. The knobs that matter:

| key | default | meaning |
|-----|---------|---------|
| `source.{host,port,db,password}` | — | PikiwiDB master (PB port = redis port + 2000) |
| `source.{local_ip,local_port}` | — | identity the master dials back to as a slave |
| `sink.kafka` / `sink.file` / `pipeline` | — | exactly one sink: Kafka topic, JSONL debug file, or the row pipeline (postgres / mysql / doris) |
| `checkpoint` | `pikawire.checkpoint.json` | crash-safe delivery position (atomic tmp+rename+fsync) |
| `dump_root` | `pikawire-dump` | working dir for the fetched dbsync dump |
| `snapshot_bgsave` | `auto` | `auto` reuses a master checkpoint the master itself deems valid; `force` demands a fresh BGSAVE |
| `buffer_dir` | beside the checkpoint | segmented on-disk backlog for binlog arriving during the dump |
| `pending_bytes_high` / `reserve_free_bytes` | 8 GiB / 4 GiB | backlog backpressure watermarks (block, never fail) |
| `buffer_fsync` | `none` | `durable`: fsync the backlog and enable crash-resume of an interrupted snapshot |
| `include_ttl` | false | emit `PEXPIRE` companion events for snapshot TTLs |
| `pipeline.sequence_column` | — | Doris MoW sequence column (guards delete->recreate ordering) |
| `heartbeat` / `ack_every` | 30s / 1000 | heartbeat period / binlog ack batch |
| `metrics_addr` | — | `/metrics`, `/healthz`, `/debug/pprof` |

## Development

```bash
make all            # vet + tests + dbsync build (needs pikiwidb deps once, cached)
make build-tools    # protocol/debug tools only (pure Go)
go test ./...       # tagless build: everything but the dump readers
```

The verification toolchain (`tools/expkit`: loader/churn/netstorm/audit/
compare/replay/...) and `pikatool` (`peek`, `fetchdump`, `verifykafka`)
re-run the live matrix from [docs/verification.md](docs/verification.md);
`demo/docker-compose.yml` brings up a full pika + kafka + tool playground.

Repo layout: `cmd/` binaries, `internal/` engine (replica, dbsync, sinks,
store, app), `drivers/` per-version C++ dump sidecars, `docs/` design,
wire-format and verification notes, `proto/` upstream protocol copies.

## License

Apache-2.0. Replication protocol behavior validated against pikiwidb
`unstable` source (v3.5.6 / 4.0.x); see [docs/design.md](docs/design.md)
for the protocol notes.
