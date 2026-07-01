//! Provenance & Evolution Catalog (blueprint §4.5).
//!
//! Every piece of evolution state — proposals, applied changes, the active
//! genome, telemetry, the workload — is first-class, queryable Postgres data.
//! This module is the typed read/write layer over the `pistol.*` tables.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::genome::{Genome, IndexSpec};

/// A concrete, EXPLAIN-able representative query with its call frequency.
#[derive(Debug, Clone)]
pub struct WorkloadQuery {
    pub fingerprint: String,
    pub query_text: String,
    pub weight: f64,
    pub label: Option<String>,
}

/// Per-table write pressure, used for the write-amplification penalty.
#[derive(Debug, Clone, Default)]
pub struct TableStat {
    pub writes: i64,
    #[allow(dead_code)] // surfaced for future fitness terms / reporting
    pub live_rows: i64,
}

// --------------------------------------------------------------------------
// Workload catalog
// --------------------------------------------------------------------------

pub async fn fetch_workload(pool: &PgPool) -> anyhow::Result<Vec<WorkloadQuery>> {
    let rows = sqlx::query("SELECT fingerprint, query_text, weight, label FROM pistol.workload")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| WorkloadQuery {
            fingerprint: r.get("fingerprint"),
            query_text: r.get("query_text"),
            weight: r.get("weight"),
            label: r.get("label"),
        })
        .collect())
}

pub async fn upsert_workload(pool: &PgPool, q: &WorkloadQuery) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO pistol.workload (fingerprint, query_text, weight, label, updated_at)
         VALUES ($1, $2, $3, $4, now())
         ON CONFLICT (fingerprint) DO UPDATE
           SET query_text = EXCLUDED.query_text,
               weight = EXCLUDED.weight,
               label = EXCLUDED.label,
               updated_at = now()",
    )
    .bind(&q.fingerprint)
    .bind(&q.query_text)
    .bind(q.weight)
    .bind(&q.label)
    .execute(pool)
    .await?;
    Ok(())
}

// --------------------------------------------------------------------------
// Telemetry snapshots
// --------------------------------------------------------------------------

pub async fn insert_telemetry(
    pool: &PgPool,
    table_stats: &Value,
    index_stats: &Value,
    query_stats: &Value,
) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO pistol.telemetry_snapshots (table_stats, index_stats, query_stats)
         VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(table_stats)
    .bind(index_stats)
    .bind(query_stats)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

// --------------------------------------------------------------------------
// Current genome
// --------------------------------------------------------------------------

pub async fn load_current_genome(pool: &PgPool) -> anyhow::Result<Genome> {
    let row = sqlx::query(
        "SELECT active_indexes FROM pistol.current_genome ORDER BY updated_at DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    match row {
        Some(r) => {
            let v: Value = r.get("active_indexes");
            // Propagate corruption instead of masking it as an empty genome,
            // which would otherwise drive duplicate proposals / wrong policy.
            let indexes: Vec<IndexSpec> = serde_json::from_value(v).map_err(|e| {
                anyhow::anyhow!("corrupt pistol.current_genome.active_indexes: {e}")
            })?;
            Ok(Genome { indexes })
        }
        None => Ok(Genome::default()),
    }
}

pub async fn save_current_genome(
    pool: &PgPool,
    genome: &Genome,
    fitness_snapshot: &Value,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO pistol.current_genome (active_indexes, fitness_snapshot, updated_at)
         VALUES ($1, $2, now())",
    )
    .bind(serde_json::to_value(&genome.indexes)?)
    .bind(fitness_snapshot)
    .execute(pool)
    .await?;
    Ok(())
}

// --------------------------------------------------------------------------
// Proposals
// --------------------------------------------------------------------------

pub async fn insert_proposal(
    pool: &PgPool,
    id: &str,
    change_type: &str,
    target: &str,
    proposal_json: &Value,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO pistol.proposals (id, status, change_type, target_object, proposal_json)
         VALUES ($1, 'proposed', $2, $3, $4)
         ON CONFLICT (id) DO UPDATE
           SET proposal_json = EXCLUDED.proposal_json, updated_at = now()",
    )
    .bind(id)
    .bind(change_type)
    .bind(target)
    .bind(proposal_json)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_proposal_decision(
    pool: &PgPool,
    id: &str,
    status: &str,
    evaluation_results: &Value,
    policy_decision: &Value,
) -> anyhow::Result<()> {
    let res = sqlx::query(
        "UPDATE pistol.proposals
            SET status = $2, evaluation_results = $3, policy_decision = $4, updated_at = now()
          WHERE id = $1",
    )
    .bind(id)
    .bind(status)
    .bind(evaluation_results)
    .bind(policy_decision)
    .execute(pool)
    .await?;
    expect_one(res, "set_proposal_decision")
}

/// Return an error unless exactly one row was updated — a missing target row
/// means provenance would silently drift out of sync.
fn expect_one(result: sqlx::postgres::PgQueryResult, what: &str) -> anyhow::Result<()> {
    match result.rows_affected() {
        1 => Ok(()),
        n => Err(anyhow::anyhow!(
            "{what}: expected to update 1 row, updated {n}"
        )),
    }
}

pub async fn link_proposal_history(
    pool: &PgPool,
    id: &str,
    history_id: i64,
    status: &str,
) -> anyhow::Result<()> {
    let res = sqlx::query(
        "UPDATE pistol.proposals
            SET status = $2, applied_history_id = $3, updated_at = now()
          WHERE id = $1",
    )
    .bind(id)
    .bind(status)
    .bind(history_id)
    .execute(pool)
    .await?;
    expect_one(res, "link_proposal_history")
}

// --------------------------------------------------------------------------
// Evolution history (immutable audit log)
// --------------------------------------------------------------------------

pub struct NewHistory {
    pub proposal_id: String,
    pub change_type: String,
    pub target_object: String,
    pub ddl_executed: String,
    pub rationale: String,
    pub before_metrics: Value,
    pub after_metrics: Value,
    pub actual_impact: Value,
    pub rollback_ddl: String,
    pub triggered_by: String,
    pub genome_context: Value,
}

pub async fn insert_history(pool: &PgPool, h: &NewHistory) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO pistol.evolution_history
           (proposal_id, change_type, target_object, ddl_executed, rationale,
            before_metrics, after_metrics, actual_impact, rollback_ddl,
            triggered_by, genome_context, status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'applied')
         RETURNING id",
    )
    .bind(&h.proposal_id)
    .bind(&h.change_type)
    .bind(&h.target_object)
    .bind(&h.ddl_executed)
    .bind(&h.rationale)
    .bind(&h.before_metrics)
    .bind(&h.after_metrics)
    .bind(&h.actual_impact)
    .bind(&h.rollback_ddl)
    .bind(&h.triggered_by)
    .bind(&h.genome_context)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

pub async fn mark_history_rolledback(
    pool: &PgPool,
    id: i64,
    actual_impact: &Value,
) -> anyhow::Result<()> {
    let res = sqlx::query(
        "UPDATE pistol.evolution_history
            SET status = 'rolled_back', actual_impact = $2
          WHERE id = $1",
    )
    .bind(id)
    .bind(actual_impact)
    .execute(pool)
    .await?;
    expect_one(res, "mark_history_rolledback")
}

#[derive(Debug)]
pub struct HistoryView {
    pub id: i64,
    pub applied_at: DateTime<Utc>,
    pub change_type: String,
    pub target_object: Option<String>,
    pub status: String,
    pub rationale: Option<String>,
    pub ddl_executed: Option<String>,
    pub rollback_ddl: Option<String>,
    pub actual_impact: Option<Value>,
}

pub async fn fetch_history(pool: &PgPool, limit: i64) -> anyhow::Result<Vec<HistoryView>> {
    let rows = sqlx::query(
        "SELECT id, applied_at, change_type, target_object, status, rationale,
                ddl_executed, rollback_ddl, actual_impact
           FROM pistol.evolution_history
          ORDER BY id DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(history_view).collect())
}

pub async fn fetch_history_by_id(pool: &PgPool, id: i64) -> anyhow::Result<Option<HistoryView>> {
    let row = sqlx::query(
        "SELECT id, applied_at, change_type, target_object, status, rationale,
                ddl_executed, rollback_ddl, actual_impact
           FROM pistol.evolution_history WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(history_view))
}

fn history_view(r: sqlx::postgres::PgRow) -> HistoryView {
    HistoryView {
        id: r.get("id"),
        applied_at: r.get("applied_at"),
        change_type: r.get("change_type"),
        target_object: r.get("target_object"),
        status: r.get("status"),
        rationale: r.get("rationale"),
        ddl_executed: r.get("ddl_executed"),
        rollback_ddl: r.get("rollback_ddl"),
        actual_impact: r.get("actual_impact"),
    }
}

// --------------------------------------------------------------------------
// Policy overrides
// --------------------------------------------------------------------------

pub async fn load_policy_overrides(pool: &PgPool) -> anyhow::Result<HashMap<String, Value>> {
    let rows = sqlx::query("SELECT key, value FROM pistol.policies")
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<String, _>("key"), r.get::<Value, _>("value")))
        .collect())
}
