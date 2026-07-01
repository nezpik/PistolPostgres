//! Evaluation Harness (blueprint §4.3) — the deterministic safety gate.
//!
//! Uses hypopg to test a *hypothetical* index at zero cost: it only affects the
//! planner, never touches real data. We compare `EXPLAIN (FORMAT JSON)` total
//! cost across the representative workload with and without the candidate, and
//! reject anything that regresses a query beyond the policy threshold.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Row};

use crate::catalog::WorkloadQuery;
use crate::genome::IndexSpec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDelta {
    pub fingerprint: String,
    pub label: Option<String>,
    pub weight: f64,
    pub baseline_cost: f64,
    pub candidate_cost: f64,
    pub improvement_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evaluation {
    /// Weighted total-cost reduction across the workload (%). Higher is better.
    pub predicted_improvement_pct: f64,
    /// Worst single-query regression (%). Positive means a query got costlier.
    pub worst_regression_pct: f64,
    pub storage_bytes: i64,
    pub baseline_cost: f64,
    pub candidate_cost: f64,
    pub per_query: Vec<QueryDelta>,
}

pub struct Evaluator<'a> {
    pool: &'a PgPool,
    workload: Vec<WorkloadQuery>,
    baseline: HashMap<String, f64>,
    baseline_total: f64,
}

impl<'a> Evaluator<'a> {
    /// Precompute baseline plan costs once (no hypothetical indexes present).
    pub async fn new(
        pool: &'a PgPool,
        workload: &[WorkloadQuery],
    ) -> anyhow::Result<Evaluator<'a>> {
        let mut conn = pool.acquire().await?;
        hypopg_reset(&mut conn).await?;
        let mut baseline = HashMap::new();
        let mut baseline_total = 0.0;
        for q in workload {
            let cost = plan_total_cost(&mut conn, &q.query_text).await?;
            baseline.insert(q.fingerprint.clone(), cost);
            baseline_total += cost * q.weight;
        }
        Ok(Evaluator {
            pool,
            workload: workload.to_vec(),
            baseline,
            baseline_total,
        })
    }

    /// Evaluate a candidate index against the whole workload via hypopg.
    pub async fn evaluate(&self, index: &IndexSpec) -> anyhow::Result<Evaluation> {
        let mut conn = self.pool.acquire().await?;
        hypopg_reset(&mut conn).await?;

        // hypopg (1.4.0) can't resolve schema-qualified names in its CREATE
        // INDEX parser, so we pass an unqualified table name and rely on the
        // connection's search_path (which resolves the target schema). Set the
        // search_path on your connection string for non-`public` schemas.
        // Run the fallible evaluation inside a block so we can ALWAYS reset
        // hypopg before returning the connection to the pool — otherwise an
        // error mid-loop would leave hypothetical indexes contaminating the
        // planner on the next reuse of this pooled session.
        let computed: anyhow::Result<(i64, Vec<QueryDelta>, f64, f64)> = async {
            // Create the hypothetical index (hypopg rejects CONCURRENTLY).
            let relid: i64 = sqlx::query_scalar(
                "SELECT indexrelid::bigint AS relid FROM hypopg_create_index($1)",
            )
            .bind(index.create_ddl_hypopg())
            .fetch_one(&mut *conn)
            .await?;

            let storage_bytes: i64 = sqlx::query_scalar("SELECT hypopg_relation_size($1::oid)")
                .bind(relid)
                .fetch_one(&mut *conn)
                .await?;

            let mut per_query = Vec::new();
            let mut candidate_total = 0.0;
            let mut worst_regression_pct: f64 = 0.0;

            for q in &self.workload {
                let baseline_cost = *self.baseline.get(&q.fingerprint).unwrap_or(&0.0);
                let candidate_cost = plan_total_cost(&mut conn, &q.query_text).await?;
                candidate_total += candidate_cost * q.weight;

                let improvement_pct = if baseline_cost > 0.0 {
                    (baseline_cost - candidate_cost) / baseline_cost * 100.0
                } else {
                    0.0
                };
                worst_regression_pct = worst_regression_pct.max(-improvement_pct);
                per_query.push(QueryDelta {
                    fingerprint: q.fingerprint.clone(),
                    label: q.label.clone(),
                    weight: q.weight,
                    baseline_cost,
                    candidate_cost,
                    improvement_pct,
                });
            }
            Ok((
                storage_bytes,
                per_query,
                candidate_total,
                worst_regression_pct,
            ))
        }
        .await;

        let reset = hypopg_reset(&mut conn).await;
        let (storage_bytes, per_query, candidate_total, worst_regression_pct) = computed?;
        reset?;

        let predicted_improvement_pct = if self.baseline_total > 0.0 {
            (self.baseline_total - candidate_total) / self.baseline_total * 100.0
        } else {
            0.0
        };

        Ok(Evaluation {
            predicted_improvement_pct,
            worst_regression_pct,
            storage_bytes,
            baseline_cost: self.baseline_total,
            candidate_cost: candidate_total,
            per_query,
        })
    }
}

async fn hypopg_reset(conn: &mut PgConnection) -> anyhow::Result<()> {
    sqlx::query("SELECT hypopg_reset()")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Parse the top-plan-node total cost out of `EXPLAIN (FORMAT JSON)`.
pub async fn plan_total_cost(conn: &mut PgConnection, sql: &str) -> anyhow::Result<f64> {
    let row = sqlx::query(&format!("EXPLAIN (FORMAT JSON) {sql}"))
        .fetch_one(&mut *conn)
        .await?;
    // The QUERY PLAN column is json; fall back to text just in case.
    let v: serde_json::Value = match row.try_get::<serde_json::Value, _>(0) {
        Ok(v) => v,
        Err(_) => {
            let s: String = row.try_get(0)?;
            serde_json::from_str(&s)?
        }
    };
    Ok(v.get(0)
        .and_then(|p| p.get("Plan"))
        .and_then(|p| p.get("Total Cost"))
        .and_then(|c| c.as_f64())
        .unwrap_or(f64::MAX))
}
