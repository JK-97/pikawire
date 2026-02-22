# Verification & testing guide

How Pikawire's consistency claims are established, and how to re-run the
matrix yourself with the tooling in this repository
(`tools/expkit/`, `cmd/pikatool/`).

## Protocol findings (validated against PikiwiDB 3.5.6 / 4.0.2)

- PB replication port = redis port + 2000; handshake `MetaSync` → `TrySync`
  → first `BinlogSync` ack with `first_send=true` activates streaming;
- binlog offset spaces: the item header carries the entry START, the
  PB-level `binlog_offset` the entry END (contiguous: next start ==
  previous end); gate/ack use the END space;
- the master reuses a previous DBSync checkpoint when the gap is within
  `kDBSyncMaxGap` (50 files) — `DBSync` therefore self-validates the
  requested position and rejects stale anchors;
- the stream must be attached AFTER the DBSync connection closes, or the
  master reports success but silently withholds pushes;
- TTL expiry emits no binlog event (both versions) — see README
  "Semantics" for the consequences;
- pika 4.0.x opening a 3.5.x data directory starts an EMPTY database
  beside the old files instead of failing — verify the layout before
  switching a running master's image.

## What is verified

| property | method | tools |
|----------|--------|-------|
| no loss / no ghost / no reordering in the incremental stream | capture the master's binlog with an *independent* replication client, then audit the emitted event stream against that ground truth (per-key order + set membership + capture-gap continuity) | `binlogcat` + `audit` |
| snapshot + handoff correctness under write storms | preload N×per-type keys, run an all-command storm (`churn`, incl. renames/TTL/type-flips) spanning bgsave, transfer, emit and drain, then audit | `loader`, `churn`, `audit` |
| sink-side convergence | replay the event stream into a clean target and diff every key's type and value against the source | `replay`, `compare` |
| event-stream semantics to Kafka | consume the topic from offset 0, replay each watched key's events (sorted by phase/position/seq to cross partition boundaries) into an operation model, and compare the model against the source's live state | `pikatool verifykafka` |
| Doris row pipeline | run the same storm through the pipeline sink, then export target rows and source keys as TSV and diff | `doris` DDL + SQL export + `dumptsv` |
| durable crash-resume | `buffer_fsync: durable` + SIGKILL the process mid-snapshot; a restart must log `RESUMING snapshot durably`, continue the dump from the persisted cursor, re-attach the stream at the fsync'd backlog tail, and complete with zero re-fetch; an unusable coordinate set must fall back to a full re-scan | SIGKILL + logs + `compare` |
| net-zero behavior of deletes | every storm operation is paired (delete-existing → restore-with-exact-value; create-temp → delete); after the storm the key count returns to its preload and per-key verification still passes | `netstorm` |

## Representative results (single Linux test host, Dockerized masters)

* 1M-key / 5-type snapshot (1.0M events ≈ 400 MB): full pipeline
  (fetch → emit → drain) 11.5 s clean; 41.5 s with a concurrent ~35k ops/s
  storm (was 52.2 s before the batch/prefetch/parallel-transfer work);
* 50K preload + 10K-op net-zero storm to Kafka (Redpanda): topic totals
  exact (70,000 events = 50,000 snapshot + 20,000 commands), per-key
  final-state check of 55,000 keys (including temp-key absence):
  0 mismatches; snapshot emission 714 ms (~70k events/s);
* same storm through the Doris pipeline (2.1, sequence column enabled):
  all 20,000 mapped rows (string+hash) byte-identical between source and
  target (0 diff lines); list/set/zset have no row mapping by design and
  are covered per-key by the Kafka checks;
* durable crash-resume: interrupted at ~3% into the dump under a storm;
  resumed from cursor + fsync'd backlog, final compare clean
  (946,998 keys, 0 value/type diffs);
* steady-state RSS ~50 MB; ~420 MB during a 6M-key dump under sustained
  45k ops/s (segmented disk backlog; backpressure blocks instead of
  growing memory).

## Re-running

```bash
# deterministic mixed-type preload + all-command storm
go run ./tools/expkit/cmd/loader    -addr 127.0.0.1:9221 -per-type 10000 ...
go run ./tools/expkit/cmd/churn     -addr 127.0.0.1:9221 -workers 6 ...
go run ./tools/expkit/cmd/netstorm  -addr 127.0.0.1:9221 -ops 10000 ...

# ground-truth capture + audit
go run ./tools/expkit/cmd/binlogcat -repl 127.0.0.1:11221 -db db0 -start <anchor> ...
go run ./tools/expkit/cmd/audit     -gt capture.jsonl -events out.jsonl -l0 <anchor>

# convergence
go run ./tools/expkit/cmd/replay  -addr 127.0.0.1:9222 -events out.jsonl -dedupe
go run ./tools/expkit/cmd/compare -source 127.0.0.1:9221 -target 127.0.0.1:9222

# sinks
go run ./cmd/pikatool verifykafka -broker 127.0.0.1:9092 -topic <t> -pika 127.0.0.1:9221 -watch 'str:0-9999:1' ...
go run ./tools/expkit/cmd/dumptsv -addr 127.0.0.1:9221 -out source.tsv   # vs SQL export
```

Unit-level guarantees (gate/drain ordering, backlog accounting, checkpoint
crash windows, resume coordinates, sink shapes) are covered by the regular
`go test ./...` suite.
