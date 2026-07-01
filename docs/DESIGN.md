# PistolPostgres — Design Notes

This document explains how the prototype works and the decisions behind it. It
complements the original vision in [`../PistolPostgres.md.txt`](../PistolPostgres.md.txt).

## Goals

1. Demonstrate **controlled evolutionary self-optimization** on an unmodified
   Postgres, end-to-end, for index evolution.
2. Make the "**non-deterministic yet reliable**" property concrete: randomness
   confined to a cheap, reproducible proposal phase; a deterministic evaluation
   + policy gate; a reversible apply.
3. Keep every component structured so broader change types (materialized views,
   partitions) slot in without reworking the pipeline.

## Why Rust

The engine is a long-running sidecar whose most critical code issues DDL against
a live database. Rust's type system and explicit error handling make the
apply/rollback path auditable, and it unifies with the blueprint's Phase-3
`pgrx` in-core extension. `tokio` gives a low-overhead resident service.

## The loop (`src/engine.rs::run_once`)

```
telemetry.collect            snapshot pg_stat_*, load workload catalog
   │
candidates_validated         parse workload SQL → per-table indexable columns,
   │                         pruned to columns that really exist
Evaluator::new               precompute baseline plan costs (once)
   │
Proposer::propose            genetic search over index designs, each scored via
   │                         the hypopg evaluator + multi-objective fitness
policy.decide                hard gates + autonomy level
   │
apply.apply_and_monitor      CREATE INDEX CONCURRENTLY, measure real impact,
   │                         auto-rollback on regression
catalog.insert_history       immutable provenance; update active genome
```

One cycle applies at most the single best proposal — "one precise shot".

## Data model (`migrations/0001_catalog.sql`)

All state is queryable Postgres data under schema `pistol`:

- `proposals` — queue/history of proposals with evaluation + policy decision.
- `evolution_history` — **immutable** audit log (an `UPDATE` trigger forbids
  mutating core columns; rows may only flip `applied → rolled_back`).
- `current_genome` — snapshots of the active physical design + fitness.
- `telemetry_snapshots` — captured `pg_stat_*` digests.
- `workload` — concrete, EXPLAIN-able representative queries with weights.
  Needed because hypopg must plan *concrete* SQL and `pg_stat_statements` only
  keeps normalized text.
- `policies` — declarative gate overrides layered on top of `pistol.toml`.

## Candidate extraction (`src/telemetry.rs`)

Rather than guessing, we parse each workload query with `sqlparser` and attribute
columns to base tables (resolving aliases and JOINs):

- equality / `IN` / `GROUP BY` columns → strong leading-column candidates,
- inequalities → range candidates,
- `ORDER BY` keys → trailing sort candidates (with direction).

Candidates are then **validated against `information_schema.columns`** so that
SELECT-list aliases leaking through `ORDER BY <alias>` (e.g. `... AS total`) can
never produce an unbuildable index.

## Evolutionary proposer (`src/proposer/evolutionary.rs`)

- An *individual* is one `IndexSpec` (an ordered column list on a table).
- **Seed** the population from candidate columns (single eq col, eq pairs, eq +
  trailing sort col, lone leading sort col).
- **Mutation**: add/drop a column, swap column order, toggle sort direction.
- **Crossover**: combine leading columns of one parent with the rest of another
  (same table).
- **Selection**: tournament; **survival**: elitist, truncated to
  `population_size`.
- **Reproducibility**: a `ChaCha8Rng` seeded from `evolution.seed`; all ranking
  ties break on index signature, so a seed fully determines the output
  regardless of `HashMap`/`HashSet` iteration order.

### Fitness (multi-objective, `score()`)

```
fitness = w_cost · improvement            (weighted plan-cost reduction)
        − w_storage · storage_penalty      (hypopg size estimate)
        − w_write_amp · write_penalty       (table write pressure × #columns)
        − w_redundancy · redundancy         (prefix overlap with active genome)
        − regression_penalty                (soft; hard gate lives in policy)
```

Weights are configurable in `pistol.toml`. This is why the search often prefers
a lean single-column index over a wider one when the extra column barely moves
cost — it balances benefit against maintenance/storage.

## Evaluation harness (`src/evaluator.rs`)

For a candidate index: `hypopg_create_index` (a hypothetical index visible only
to the planner), then `EXPLAIN (FORMAT JSON)` each workload query and read the
top node's `Total Cost`; `hypopg_relation_size` estimates storage; `hypopg_reset`
cleans up. Improvement is the weighted total-cost delta; the worst per-query
regression is tracked for the gate. Zero rows are written and no real index is
built.

### hypopg quirk (important)

hypopg 1.4.0's `hypopg_create_index` parser cannot resolve **schema-qualified**
table names, and (surprisingly) fails when `search_path` is *explicitly* set to
the target schema, yet resolves fine under the connection's default
`search_path`. So the evaluator passes an **unqualified** table name
(`IndexSpec::create_ddl_hypopg`) and relies on the connection's `search_path`.
For non-`public` schemas, set the schema on your connection string
(`?options=-csearch_path=myschema,public`). Real DDL (`CREATE INDEX
CONCURRENTLY`, `pg_relation_size`) uses normal schema-qualified names.

## Apply & feedback (`src/apply.rs`)

`CREATE INDEX CONCURRENTLY` cannot run inside a transaction block, so it is sent
via the simple query protocol (passing `&str` to sqlx's `execute`) on a
dedicated pooled connection. The rollback DDL (`DROP INDEX CONCURRENTLY IF
EXISTS`) is computed and stored **before** apply. After building, we `ANALYZE`
the table and re-measure real plan costs; if the worst regression exceeds the
gate, we immediately run the rollback. A failed concurrent build (which can
leave an `INVALID` index) is cleaned up.

## Policy (`src/policy.rs`)

Pure decision function — never touches the DB. Gates: protected schema/table,
identical-index dedup, min predicted improvement, max regression, per-table
index cap, daily storage budget. Autonomy levels: `advisory` (record only),
`auto_safe`, `auto_broad`. Overrides from `pistol.policies` layer over config.

## Testing

- **Unit** (`src/genome.rs`, `src/policy.rs`): identifier bounds/determinism,
  DDL shaping, overlap detection, every policy gate, override merging.
- **Property** (`tests/property.rs`, `proptest`): index-name bounds &
  determinism, DDL well-formedness, overlap reflexivity across random specs.
- **Integration** (`tests/integration.rs`, gated on `PISTOL_TEST_DATABASE_URL`):
  migrations, candidate extraction, **seed reproducibility**, a full applying
  cycle with provenance, and the **auto-rollback** path (forced with a negative
  regression gate).

## Known limitations / future work

- Change types limited to B-tree indexes; MV/partition/schema-extension seams
  exist but are unimplemented.
- Cost model is the Postgres planner's estimate (via hypopg), not measured
  latency; a replay/branch-based harness (blueprint §4.3) would tighten this.
- No Supabase branch integration, no learned cost models, no pgrx extension yet
  (blueprint Phases 2–3).
- Single-node, single evolution loop; multi-tenant per-tenant policies are
  future work.
