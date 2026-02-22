# Embedded SQL pipeline (Phase 2)

For teams that don't want Kafka, Pikawire can apply declarative transforms
and write rows directly to a relational target:

```yaml
source: { host: pika, port: 9221, db: db0 }
pipeline:
  driver: postgres            # or mysql
  dsn: "host=pg user=... dbname=... sslmode=disable"
  flush_every: 100ms          # group-commit period
  max_batch_rows: 500         # group-commit trigger
  rules:
    - types: [hash]
      key_pattern: "user:{uid}"       # {col} captures become column values
      table: ods_user
      pk: [uid]
      fields: { name: name, age: age } # hash-field -> column whitelist
      delete: row                      # row (default) | ignore
    - types: [string]
      key_pattern: "acct:{id}"
      table: ods_account
      pk: [id]
      value_json: true                 # expand JSON string values
      json_columns: [balance, name]    # optional whitelist
checkpoint: pikawire.checkpoint.json
```

## Semantics

- **Mapping is stateless per event.** `key_pattern` captures columns from the
  key; `fields` whitelists hash fields; `value_json` expands JSON strings.
- **Deletes**: binlog DEL carries no data type, so delete events match rules
  by `key_pattern` across all rules (idempotent on non-owning tables).
  `delete: ignore` turns them into no-ops for log-style targets.
- **Consistency**: identical anchor/checkpoint machinery as the Kafka sink
  (see design.md). SQL commits use the same ordering: snapshot full images
  first, then binlog order — per key.
- **Throughput**: events are grouped into transactions by the committer;
  same-pk mutations within a group are coalesced (last-writer-wins, delete
  collapse) to reduce write amplification during catch-up. `Emit` blocks until
  its group commits, so the delivery checkpoint stays honest.
- **No automatic DDL**: create target tables beforehand. Columns absent from
  an event keep their stored value (partial upsert semantics of
  `ON CONFLICT DO UPDATE SET <only present columns>`).
- **v1 limits**: list/set/zset types and HDEL field-nulling are not mapped
  (use the Kafka path with your own consumer for those). TTL expiry is not
  observable in the binlog (documented limitation).

## Verified

Live end-to-end against pikiwidb 3.5.6 → postgres 16: 20-key snapshot, live
HSET updates (renamed values), key-pattern JSON expansion (SETEX-style JSON),
DEL propagation, and crash-resume idempotency. Also live-verified against
mysql 8.0 and doris 2.1 (see verification.md).

## Design lineage

For contributors evaluating "which upstream should this behave like":

- **Mapping layer** (`key_pattern` / `fields` / `pk` / `delete`): modeled on
  Debezium Server JDBC sink and Airbyte destination-jdbc (primary-key &
  delete-row semantics). Key-pattern captures (`user:{uid}`) are
  Pikawire-original because no upstream schema exists; the filter/whitelist
  config shape borrows from Maxwell.
- **Batching layer**: coalescing per flush (last-writer-wins, delete
  collapse) follows ClickPipes / doris-flink-connector
  (`sink.buffer-flush.*`); the group-commit handoff (`Emit` blocks until its
  group commits) is the WAL-fsync batching idiom.
- **Dialect layer**: Flink CDC 3.x pipeline connectors'
  source/transform/sink structure; upsert statement shapes from DataX
  rdbwriter and Debezium JDBC (`ON CONFLICT DO UPDATE SET col=EXCLUDED.col`
  for Postgres, `ON DUPLICATE KEY UPDATE col=VALUES(col)` for MySQL), SET
  restricted to columns present in the event.
- **Deliberately NOT adopted**: schema auto-evolution (Kafka Connect
  `auto.evolve`, Flink CDC `schema.change.behavior=EVOLVE`). Targets are
  pre-created and mapping errors fail loudly — consistent with Debezium's
  own production guidance.
- **Doris MoW ordering**: with `partial_columns` loads, a delete-sign batch
  can swallow the next partial upsert of the same key (reproduced on
  Doris 2.1: delete -> recreate rows lost). Set
  `pipeline.sequence_column: <col>` and give every target table a
  `BIGINT` column with `'function_column.sequence_type'='bigint'`: the sink
  then stamps every load row with the event's monotonic `(filenum, offset)`
  and declares `function_column.sequence_col`, so apply order is decided by
  the source position, not by load timing.
- **Doris specifics**: `partial_columns:true` with a columns-subset per load
  (doris-flink-connector practice); one column shape per load group because
  undeclared columns are NULL-filled by Stream Load — verified live on 2.1,
  the bug the connector docs warn about.
- **Contract**: at-least-once + idempotent upsert; ordering defense lives in
  the envelope `(filenum, offset, seq)` — targets may adopt
  `__DORIS_SEQUENCE_COL__` or version columns. Same stance as Maxwell /
  Debezium JDBC.

One-line genealogy: mapping from Debezium-JDBC, batching from
ClickPipes/doris-connector, dialects from Flink-CDC-3.x — and the schema
evolution parts we intentionally did not copy.
