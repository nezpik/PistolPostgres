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

## Measured gate (`src/measure.rs`, `engine.rs::measured_trial`) — the decision-maker

hypopg's estimated cost is excellent for cheaply *ranking* hundreds of
candidates, but planner cost units are only loosely correlated with wall-clock
latency — gating on them alone would let confidently-wrong changes through. So
the loop is **two-tier**:

- **Tier 1 (estimated, no impact):** the evolutionary search + a policy
  pre-filter, both on hypopg cost. Picks the single best candidate.
- **Tier 2 (measured, reversible):** build the winner, then measure real
  `EXPLAIN (ANALYZE)` latency across the weighted workload and keep it only if
  the measurement agrees.

Making measurement reliable is the hard part. Three techniques:

1. **Best-of-N, not mean/median.** Interference (autovacuum, other queries) can
   only ever *add* time, so the fastest observed run is the least-noisy estimate
   of true cost. We run `samples + 1` times, discard the cold warm-up, and take
   the minimum.
2. **Noise floor.** A query whose baseline is below `noise_floor_ms` is too fast
   to time reliably (sub-millisecond jitter reads as a huge %), so it cannot
   veto a change. Regression also requires an absolute slowdown above the floor,
   not just a relative %.
3. **Realistic in-place tolerance.** Building the index evicts buffer-cache
   pages, so unrelated queries can read slightly slower in the post-build
   window. An in-place trial therefore uses a tolerant `max_measured_regression_pct`
   that catches only egregious (plan-flip) regressions. Pointing
   `shadow_database_url` at a quiet replica/branch removes this perturbation and
   lets you tighten the threshold toward a few percent — and gives **zero
   production impact**, since the trial builds/measures/drops on the replica and
   only applies to the primary if the replica passes.

Provenance records both `predicted_improvement_pct` (hypopg) and the full
`measured` impact, so the estimate-vs-reality gap is always visible in
`pistol.evolution_history`.

## Apply primitives (`src/apply.rs`)

`build_index_online` / `drop_index_online` are the online, reversible building
blocks the measured trial composes. `CREATE INDEX CONCURRENTLY` cannot run
inside a transaction block, so it is sent via the simple query protocol (passing
`&str` to sqlx's `execute`) on a dedicated pooled connection, then the table is
`ANALYZE`d so the new index is actually considered. The rollback DDL (`DROP
INDEX CONCURRENTLY IF EXISTS`) is computed and stored **before** apply, and a
failed concurrent build (which can leave an `INVALID` index) is cleaned up.

## Policy (`src/policy.rs`)

Pure decision function — never touches the DB. Gates: protected schema/table,
identical-index dedup, min predicted improvement, max regression, per-table
index cap, daily storage budget. Autonomy levels: `advisory` (record only),
`auto_safe`, `auto_broad`. Overrides from `pistol.policies` layer over config.

## Testing

- **Unit** (`src/genome.rs`, `src/policy.rs`, `src/measure.rs`): identifier
  bounds/determinism, DDL shaping, overlap detection, every policy gate, override
  merging, and the measured-gate math (weighted summary, noise-floor exclusion,
  best-of-N reduction).
- **Property** (`tests/property.rs`, `proptest`): index-name bounds &
  determinism, DDL well-formedness, overlap reflexivity across random specs.
- **Integration** (`tests/integration.rs`, gated on `PISTOL_TEST_DATABASE_URL`):
  migrations, candidate extraction, **seed reproducibility**, a directly-measured
  index win, a full applying cycle validated by **measured latency** with
  predicted-vs-measured provenance, and the **measured auto-rollback** path
  (forced with an unmeetable improvement bar).

## Known limitations / future work

- Change types limited to B-tree indexes; MV/partition/schema-extension seams
  exist but are unimplemented.
- The measured gate uses real `EXPLAIN (ANALYZE)` latency, but on the *registered
  representative* workload; automatic capture of concrete production queries
  (via `auto_explain`/sampling) is the next step to true self-driving.
- Shadow measurement expects an operator-provided replica/branch URL; we don't
  yet provision it (blueprint's Supabase-branch integration, Phase 2).
- No learned cost models, no pgrx in-core extension yet (blueprint Phase 3).
- Single-node, single evolution loop; multi-tenant per-tenant policies are
  future work.
