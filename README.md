# PistolPostgres

**Precision Intelligent Self-evolving & Optimizing Layer for Postgres**

PistolPostgres turns a standard Postgres into a **self-improving** database. It
leaves the Postgres storage engine, MVCC, WAL, and planner completely untouched
(they own durability and correctness) and adds a sidecar **Evolution
Intelligence Layer** that continuously:

```
 collect telemetry ─▶ propose (evolutionary search) ─▶ Tier 1: hypopg estimate (zero-cost pre-filter)
        ▲                                                          │
        │                                                          ▼
   record + learn ◀── keep or auto-rollback ◀── Tier 2: MEASURED latency (EXPLAIN ANALYZE) ◀── policy pre-gate
```

The decision to keep a change is made on **measured latency**, not the
optimizer's estimate — hypopg only cheaply pre-filters which candidate is worth
measuring.

Every step is recorded as first-class, queryable rows in `pistol.*` catalog
tables, so the self-modification is fully auditable and reversible.

This repository is a **working Rust prototype** of the
[blueprint](./PistolPostgres.md.txt), scoped to **index evolution** (B-tree /
multicolumn) with the engine structured so partitions and materialized views
plug in later.

---

## Non-deterministic, yet reliable

The signature property. The *proposal* phase is deliberately non-deterministic:
a small genetic algorithm mutates and crosses over candidate index designs to
escape the local optima that greedy recommenders hit. But nothing random ever
reaches your data:

- **Reproducible randomness** — the search uses a *seedable* RNG. A fixed
  `evolution.seed` gives byte-for-byte identical proposals (used in tests and
  demos); seed `0` draws from OS entropy for real exploration in production.
  Ranking ties break on a stable key, so a seed fully determines the outcome.
- **Two-tier gate** — Tier 1 evaluates candidates with **hypopg** (hypothetical
  indexes that only affect the planner, cost nothing, touch no data) to cheaply
  rank them and pre-filter on predicted cost. Tier 2 then validates the winner
  on **real measured latency** (`EXPLAIN ANALYZE`, best-of-N) — because planner
  cost is only loosely correlated with wall-clock time.
- **Reversible apply** — changes go in online (`CREATE INDEX CONCURRENTLY`) and
  their rollback DDL is stored *before* apply. A change that measurably helps is
  **kept**; only a failed or non-improving trial is automatically rolled back.
  With a `shadow_database_url` (a replica/branch) the measurement runs with
  **zero production impact** — build/measure/drop happen on the replica and the
  index is applied to the primary only if it passes; otherwise it's an in-place
  trial with guaranteed rollback on failure.

Randomness explores; determinism decides.

---

## Quickstart

Requires Docker (for the bundled Postgres with `hypopg` + `pg_stat_statements`)
and a Rust toolchain.

```bash
./scripts/demo.sh
```

That brings up Postgres, builds the engine, seeds a synthetic edtech database
with hot **un-indexed** query patterns, and runs the evolution loop — showing
each proposal, the policy decision, the online apply, and the measured impact.

Or step by step:

```bash
docker compose up -d --build
export PISTOL_DATABASE_URL=postgres://pistol:pistol@127.0.0.1:55432/pistol

cargo run -- init                     # create the pistol.* evolution catalog
cargo run -- demo all --iterations 20 # schema + ~490k rows + representative workload
cargo run -- run                      # one full evolution cycle
cargo run -- status                   # active genome + counts
cargo run -- history                  # applied & rolled-back changes, with rollback DDL
```

Example cycle output:

```
▸ telemetry snapshot #11 — 6 workload queries, 15 tables
▸ 5 candidate proposal(s) after evolutionary search:
    1. fitness +0.239 | +30.8% cost | public.student_progress USING btree (student_id)
    ...
▸ policy [auto_safe]: APPLY — all gates passed
▸ applying online: CREATE INDEX CONCURRENTLY pi_student_progress_student_id_db9d7a ON ...
✓ applied pi_student_progress_student_id_db9d7a (history #1) — actual 30.9% weighted plan-cost reduction
```

---

## CLI

These subcommands double as the "Hermes tool" seam from the blueprint (§6).

| Command | Purpose |
|---|---|
| `pistol init` | Create the `pistol.*` catalog (runs migrations). |
| `pistol demo all\|schema\|seed\|load` | Build the demo edtech DB & workload. |
| `pistol capture [--min-calls N --limit N]` | Auto-populate the workload from `pg_stat_statements` (self-driving). |
| `pistol collect` | Take a telemetry snapshot; show derived index candidates. |
| `pistol propose` | Run the evolutionary search; print ranked proposals (no changes). |
| `pistol run [--watch --interval N]` | Run the full cycle once, or continuously. |
| `pistol status` | Active genome and evolution counters. |
| `pistol history [--limit N]` | Applied & rolled-back changes with rationale + rollback DDL. |
| `pistol rollback <id>` | Undo a previously applied change on demand. |

Config lives in [`pistol.toml`](./pistol.toml) (connection, evolution
parameters, fitness weights, and policy gates). Key overrides: `PISTOL_DATABASE_URL`,
`PISTOL_SEED`, `PISTOL_AUTONOMY`.

### Self-driving on your own database

No hand-written workload required — point it at a database with
`pg_stat_statements` and let it learn the real workload:

```bash
pistol init
pistol capture     # pulls the hot queries from pg_stat_statements into pistol.workload
pistol run         # proposes + evaluates + applies against that real workload
```

Captured queries are *normalized* (`… WHERE x = $1`), so the proposal/evaluation
tier plans them with `EXPLAIN (GENERIC_PLAN)` (hypopg's hypothetical indexes are
still considered). The measured Tier-2 gate applies to **concrete** queries; a
purely parameterized workload is validated on the estimated generic-plan cost
(restoring measured validation there — via concrete parameter sampling — is the
next step). History labels each change `measured` or `predicted` accordingly.

---

## How it maps to the blueprint

| Blueprint §4 component | Module |
|---|---|
| 4.1 Telemetry Collector | [`src/telemetry.rs`](./src/telemetry.rs) |
| 4.2 Proposal & Evolutionary Engine | [`src/genome.rs`](./src/genome.rs), [`src/proposer/`](./src/proposer) |
| 4.3 Evaluation Harness (hypopg) | [`src/evaluator.rs`](./src/evaluator.rs) |
| 4.4 Policy & Decision Engine | [`src/policy.rs`](./src/policy.rs) |
| 4.5 Provenance & Evolution Catalog | [`src/catalog/`](./src/catalog), [`migrations/`](./migrations) |
| 4.6 Apply & Feedback Loop | [`src/apply.rs`](./src/apply.rs), [`src/engine.rs`](./src/engine.rs) |

See [`docs/DESIGN.md`](./docs/DESIGN.md) for the deep dive.

---

## Optional: Claude-backed proposer

The evolutionary proposer is always available and fully offline. Compiling with
the `llm` feature adds an optional Claude-backed proposer — it *suggests*
candidate indexes, which are then routed through the **same** hypopg evaluator
and policy gates (the LLM never bypasses the safety path):

```bash
export ANTHROPIC_API_KEY=...      # falls back to the evolutionary proposer if unset
cargo run --features llm -- run   # with PISTOL_PROPOSER=llm to select it
```

---

## Safety model (defense in depth)

1. Tier 1 — zero-cost hypopg evaluation + declarative policy pre-filter before
   anything touches production (min improvement, per-table index caps, daily
   storage budget, protected schemas, graduated autonomy `advisory` →
   `auto_safe` → `auto_broad`).
2. Tier 2 — real measured-latency gate (best-of-N `EXPLAIN ANALYZE`, with a
   noise floor so timing jitter can't force false rollbacks); on a shadow
   replica for zero impact, else in-place with guaranteed rollback.
3. Online-first apply (`CREATE INDEX CONCURRENTLY`); never bypasses Postgres'
   transaction/recovery model.
4. Rollback DDL stored *before* apply; the trial keeps only measured-good
   changes; manual `rollback` any time.
5. Immutable, append-only audit log (enforced by a DB trigger) recording
   **predicted vs measured** for every change.

---

## Development

```bash
cargo test          # unit + property tests (no DB needed)
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check

# full DB-backed integration test:
export PISTOL_TEST_DATABASE_URL=postgres://pistol:pistol@127.0.0.1:55432/pistol
cargo test --test integration
```

CI ([`.github/workflows/ci.yml`](./.github/workflows/ci.yml)) provisions
Postgres + hypopg and runs fmt, clippy, and the whole suite including the
integration test.

---

*Prototype status: index evolution end-to-end. Materialized views, partitions,
Supabase-branch evaluation, and a pgrx in-core extension are future work — see
`docs/DESIGN.md`.*
