//! Apply & Feedback loop (blueprint §4.6). Applies a change online, stores its
//! reversal DDL, then verifies the real-world impact and auto-rolls-back on
//! regression. This is where "reliable" self-modification actually happens.

use std::collections::HashMap;

use serde_json::{json, Value};
use sqlx::{Executor, PgPool};

use crate::catalog::WorkloadQuery;
use crate::evaluator::{plan_total_cost, Evaluation};
use crate::genome::IndexSpec;

#[derive(Debug)]
pub struct ApplyOutcome {
    pub index_name: String,
    pub ddl_executed: String,
    pub rollback_ddl: String,
    pub actual_impact: Value,
    pub rolled_back: bool,
}

/// Apply the index online (`CREATE INDEX CONCURRENTLY`), then measure real plan
/// costs and roll back automatically if any query regressed beyond the gate.
pub async fn apply_and_monitor(
    pool: &PgPool,
    index: &IndexSpec,
    before_eval: &Evaluation,
    workload: &[WorkloadQuery],
    max_regression_pct: f64,
) -> anyhow::Result<ApplyOutcome> {
    let ddl_executed = index.create_ddl(true);
    let rollback_ddl = index.drop_ddl(true);
    let index_name = index.index_name();

    // CREATE INDEX CONCURRENTLY cannot run inside a transaction block, so we
    // use the simple query protocol on a dedicated connection (passing &str to
    // execute() runs it unprepared / autocommit).
    let mut conn = pool.acquire().await?;
    if let Err(e) = (&mut *conn).execute(ddl_executed.as_str()).await {
        // A failed CONCURRENTLY build can leave an INVALID index — clean it up.
        let _ = (&mut *conn).execute(rollback_ddl.as_str()).await;
        return Err(anyhow::anyhow!("apply failed: {e}"));
    }

    // Refresh planner statistics so the new index is actually considered.
    let analyze = format!("ANALYZE {}", index.qualified_table());
    let _ = (&mut *conn).execute(analyze.as_str()).await;

    // Measure real post-apply plan costs vs the pre-apply baseline.
    let before: HashMap<&str, f64> = before_eval
        .per_query
        .iter()
        .map(|d| (d.fingerprint.as_str(), d.baseline_cost))
        .collect();
    let weights: HashMap<&str, f64> = workload
        .iter()
        .map(|w| (w.fingerprint.as_str(), w.weight))
        .collect();

    let mut per_query = Vec::new();
    let mut before_total = 0.0;
    let mut after_total = 0.0;
    let mut worst_regression_pct: f64 = 0.0;

    for w in workload {
        let b = *before.get(w.fingerprint.as_str()).unwrap_or(&0.0);
        let a = plan_total_cost(&mut conn, &w.query_text).await?;
        let weight = *weights.get(w.fingerprint.as_str()).unwrap_or(&1.0);
        before_total += b * weight;
        after_total += a * weight;
        let improvement_pct = if b > 0.0 { (b - a) / b * 100.0 } else { 0.0 };
        worst_regression_pct = worst_regression_pct.max(-improvement_pct);
        per_query.push(json!({
            "fingerprint": w.fingerprint,
            "label": w.label,
            "before_cost": b,
            "after_cost": a,
            "improvement_pct": improvement_pct,
        }));
    }

    let actual_improvement_pct = if before_total > 0.0 {
        (before_total - after_total) / before_total * 100.0
    } else {
        0.0
    };

    let real_size: i64 = sqlx::query_scalar("SELECT pg_relation_size($1::regclass)")
        .bind(format!("\"{}\".\"{}\"", index.schema, index_name))
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(0);

    let mut rolled_back = false;
    if worst_regression_pct > max_regression_pct {
        // Regression detected in production — undo immediately.
        tracing::warn!(
            index = %index_name,
            worst_regression_pct,
            "post-apply regression exceeded gate; rolling back"
        );
        (&mut *conn).execute(rollback_ddl.as_str()).await?;
        let _ = (&mut *conn).execute(analyze.as_str()).await;
        rolled_back = true;
    }

    let actual_impact = json!({
        "predicted_improvement_pct": before_eval.predicted_improvement_pct,
        "actual_improvement_pct": actual_improvement_pct,
        "worst_regression_pct": worst_regression_pct,
        "real_index_size_bytes": real_size,
        "auto_rolled_back": rolled_back,
        "per_query": per_query,
    });

    Ok(ApplyOutcome {
        index_name,
        ddl_executed,
        rollback_ddl,
        actual_impact,
        rolled_back,
    })
}

/// Execute a stored rollback DDL on demand (used by `pistol rollback <id>`).
pub async fn execute_rollback(pool: &PgPool, rollback_ddl: &str) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    (&mut *conn).execute(rollback_ddl).await?;
    Ok(())
}

/// True if a physical index with this name already exists (idempotency guard).
pub async fn index_exists(pool: &PgPool, schema: &str, name: &str) -> anyhow::Result<bool> {
    let exists: Option<bool> =
        sqlx::query_scalar("SELECT true FROM pg_indexes WHERE schemaname = $1 AND indexname = $2")
            .bind(schema)
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(exists.unwrap_or(false))
}
