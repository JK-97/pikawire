# Design: consistent full + incremental sync over the PikiwiDB slave protocol

## Goal

Emit every key exactly once as a *full image* (snapshot), then a complete,
per-key-ordered incremental command stream — with no manual binlog-offset
handoff and crash-safe resumption.

## The consistency contract

The snapshot is PikiwiDB's own RocksDB checkpoint: `DBSync` asks the master
for a `bgsave`, the dump is transferred over the rsync service, and the
master records the **exact binlog position of the checkpoint** (`bgsave_end`
anchor). The dump is therefore a point-in-time consistent view, and the
incremental session `TrySync`s from that anchor. There is no reconciliation
window between full and incremental at all:

- snapshot events are the state at the anchor;
- every binlog entry after the anchor is emitted exactly once, in binlog
  order, per key;
- entries at or before the anchor are dropped by the gate (they are already
  contained in the dump images).

### snapshot_bgsave: reuse, reposition, escalate

`auto` (default) lets the master reuse an existing dbsync checkpoint while
it is inside the master's own reuse window (kDBSyncMaxGap = 50 binlog files).
If the served anchor trails the live stream start L0, the stream is
**repositioned** to the anchor — the handoff has emitted nothing yet, so
re-buffering from the anchor is gap-free. If the master already purged the
anchor, the attempt escalates to a forced fresh BGSAVE. `force` skips reuse
and demands a new checkpoint up front (max anchor freshness, one master
checkpoint per rerun). This keeps steady-state reruns from hammering the
master while preserving the coverage invariant.

### Anchor-first handoff (purge-race elimination)

The DBSync response carries no anchor (the proto only returns slot+session_id)
— but the anchor IS the master's checkpoint memory state, and pika's own
`TryDBSync` re-bgsaves whenever the existing dump is missing, its anchor
binlog file was purged, or the gap exceeds `kDBSyncMaxGap` (50 files). So we
simply order the steps so the anchor is known BEFORE the stream attaches:

  1. DBSync handshake (fast; master validates/refreshes its checkpoint);
  2. rsync meta + the small `info` file ONLY -> anchor A is now known;
  3. open the replication session with `TrySync(A)`; from here every entry
     >= A is consumed into the disk backlog and acked — correctness no
     longer depends on master-side binlog retention at all, and the open
     DBSync connection additionally freezes master purge (kSlaveDbSync);
  4. bulk-transfer the dump (stream keeps buffering concurrently), emit it,
     then `Release` drains the backlog in binlog order and opens the gate.

`snapshot_bgsave: force` prepends an explicit BGSAVE+LASTSAVE poll (anchor
near-latest, one master checkpoint per rerun); `auto` (default) accepts the
master's reused checkpoint. If A's binlog is purged between steps 1 and 3
(seconds-wide race), `TrySync` returns ErrPurged and the attempt escalates
once to forced mode.

### Disk-backed handoff backlog

While the gate buffers, entries are appended to a segmented on-disk log
(`[4B len][pos|exec|raw binlog item]`, 64MiB segments, segments unlinked as
the drain cursor passes them) instead of RAM, so writer memory stays bounded
no matter how long the drain takes (measured: ~420MB holding a 6M-key dump
against a sustained 35k+ ops/s stream). Backpressure is a byte high-watermark (default 8GiB, hysteresis
to half) plus a statfs free-space reserve (default 4GiB): consume BLOCKS
while the watermark is engaged — it never fails the run. Default fsync=none
matches the recovery model (any crash before snapshot completion re-runs the
dump, so page-cache persistence suffices).

### Durable snapshot resume (`buffer_fsync: durable`)

Durable mode changes the crash story from "re-run the whole dump" to
"continue it". Three coordinates make a snapshot resumable, persisted in the
checkpoint next to `delivered`: the **anchor** (stream re-attach point), the
**dump iterator cursor** (engine type + key, flushed every ~20k emitted
events), and a **dump-complete** flag. In addition, durable mode fsyncs
backlog records (4MB batches) and *retains* drained segments until the
snapshot's Release fully completes (close/`discard` semantics): the backlog
is then a durable, replayable prefix of the post-anchor binlog.

On restart with `snapshot=running` + resume coordinates + an intact dump
folder, the runner: rescans the retained backlog for the pb-end of its last
fsync'd record, attaches the stream **exactly there** (`TrySync` tail), and
re-enters the dump iterator with the cursor, which the embedded dumper
implements by skipping to the resume engine and `Seek(resume_key)` inclusive
— emission is idempotent per key (SET/HSET-style apply), so the re-scanned
dump records from the cursor on are safe to replay on top of whatever the
backlog already applied. The drain then proceeds from the persisted
`delivered` position as usual. If any precondition fails (no dump folder,
tail purged on the master, non-durable config), the run falls back to the
plain full re-scan — the non-durable behaviour is byte-for-byte unchanged.

## Reader driver architecture

pika's storage format is version-coupled (3.5.x: six per-type engine dirs;
4.0.x: three numeric kv/svc/meta instances, each version's own rocksdb).
Rather than embedding one pika tree in the binary, each supported major is
compiled into a self-contained **sidecar dumper**:

```
drivers/v35/driver.cc ─ link 3.5.6 libstorage.a ─> pikawire-dumper-v35 } embedded
drivers/v40/driver.cc ─ link 4.0.2 libstorage.a ─> pikawire-dumper-v40 } (go:embed)
```

- fixed framed pipe protocol (`drivers/include/pikawire_dumper_proto.h`):
  parent writes `O`pen/`N`ext/`C`lose, child replies length-prefixed record
  batches; `pikawire_driver.h` carries the in-process C ABI between a driver's
  glue and its storage layer;
- **sidecar, not dlopen**: the drivers carry the whole RocksDB + pika-storage
  world with a ~34KB TLS image containing initial-exec relocations, which
  exceeds glibc's static-TLS surplus for runtime-loaded objects ("cannot
  allocate memory in static TLS block") — a startup-exec'd process sizes it
  correctly. The process boundary additionally contains any
  LOG(FATAL)/segfault inside third-party storage code: the parent surfaces a
  clean iterator error instead of dying;
- driver selection is a layout sniff of the fetched dump's engine
  directories (deterministic, local, no server round-trip); every driver
  additionally pre-validates its layout and fails closed with a "wrong
  driver" error (pika's `Storage::Open` LOG(FATAL)s on foreign or empty
  engine dirs — the preflight also bootstraps never-written empty engines);
- the replication/emit pipeline, checkpointing and the C++-free Go tests are
  identical across majors; supporting a new pika major = new driver port +
  one line in `scripts/build-cgo.sh`.

## Emission ordering

All events funnel through ONE emit goroutine (`emitCh`) and one ordering
rule: during the dump phase the gate stays `buffering` — binlog entries
beyond the anchor are queued, never emitted, and the Release path drains
the buffer as the *single* producer before opening. (A previous revision
flipped the gate to `open` before draining, letting the receive loop emit
live entries around the backlog — per-key order collapsed under load.
Covered by `TestReleaseKeepsBufferingUntilDrained`.)

## Crash safety

- `delivered` advances only after the sink confirmed the event. The dump
  phase pins the checkpoint to the anchor before any snapshot event, so a
  crash *during* the dump re-runs the (idempotent) dump (or resumes it in
  durable mode, see above), and a crash *after* resumes from the anchor —
  gap writes made while down are replayed from the master.
- Binlog files are append-only; `TrySync` requires an entry-boundary
  position — the bgsave anchor satisfies this (PB-end == next entry start).
- The master evicts stale slaves and purges old binlog by ack watermark;
  if our resume position is purged, startup fails with `ErrPurged` and the
  operator re-snapshots.

## Protocol notes (validated against pikiwidb 3.5.6 / 4.0.2)

- PB replication port = redis port + 2000; handshake: `MetaSync` → `TrySync`
  → first `BinlogSync` ack with `first_send=true` **activates** streaming.
- **Keepalive is a repeated `TrySync`** (fire-and-forget); an all-zero
  `BinlogSync` ack is a ping. Never re-ack an already-acked range: the master
  validates both ends against its sync window and **closes the connection** on
  a miss.
- Entry positions exist in two spaces: the item header carries the entry
  **start** offset; the PB wrapper carries the entry **end** offset (== next
  entry start). Acks must use the PB-end space; events expose the entry-start
  space.
- Empty `binlog` payloads are master keepalive packets — skip.
- The `TrySync` response echoes the master's producer position; `INFO
  replication` (`db0:binlog_offset=<filenum> <offset>`) is the same anchor.
  A synthetic "max uint" tip is NOT accepted by the master.
- `SETEX` arrives as the internal command `pksetexat key expire_at value`
  (absolute time); Pikawire passes it through unmodified.
- `MSET` arrives expanded into one `set` entry per key (verified on 4.0.2).
- TTL expiry does NOT write a binlog DEL — verified; it is the known delete
  black hole (see README limitations).
- Multi-slave: the master tracks slaves by (ip, port); concurrent sessions
  (product + independent audit/capture clients) are supported.

## Sink integration (checkpoint honesty)

Kafka delivery is synchronous per event before `delivered` advances
(at-least-once). Batch throughput comes next (async pipeline with a
completion-ordered window); the single-producer ordering structure is already
in place.
