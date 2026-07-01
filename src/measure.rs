//! Measured-impact harness (the Tier-2 gate).
//!
//! hypopg (Tier 1) predicts *estimated* plan cost — great for cheaply ranking
//! hundreds of candidates, but planner cost is only loosely correlated with
//! wall-clock latency. Here we measure the *real* thing: `EXPLAIN (ANALYZE)`
//! execution time across the weighted workload, taking the best-of-N run per
//! query (the least-noisy estimate of true cost) and a noise-floor guard so
//! that timing jitter can't force a false rollback. The evolution loop keeps a
//! change only if the measured numbers agree with the prediction.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::catalog::WorkloadQuery;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTiming {
    pub fingerprint: String,
    pub label: Option<String>,
    pub weight: f64,
    pub baseline_ms: f64,
    pub candidate_ms: f64,
    pub improvement_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasuredImpact {
    /// Weighted latency reduction across the workload (%). Higher is better.
    pub improvement_pct: f64,
    /// Worst single-query latency regression (%). Positive means slower.
    pub worst_regression_pct: f64,
    pub baseline_ms: f64,
    pub candidate_ms: f64,
    pub samples: usize,
    pub per_query: Vec<QueryTiming>,
}

impl MeasuredImpact {
    /// Does this clear the measured gate?
    pub fn passes(&self, min_improvement_pct: f64, max_regression_pct: f64) -> bool {
        self.improvement_pct >= min_improvement_pct
            && self.worst_regression_pct <= max_regression_pct
    }
}

/// Measure the *best* execution time (ms) per workload query. Runs `samples + 1`
/// times, discards the first (cold-cache) run, and takes the minimum: the
/// fastest observed run is the least-noisy estimate of true cost, since
/// interference from other work can only ever add time. This is what makes
/// regression detection stable enough to gate on.
pub async fn measure(
    pool: &PgPool,
    workload: &[WorkloadQuery],
    samples: usize,
) -> anyhow::Result<HashMap<String, f64>> {
    let samples = samples.max(1);
    let mut out = HashMap::new();
    let mut conn = pool.acquire().await?;
    for q in workload {
        let mut best = f64::MAX;
        for i in 0..(samples + 1) {
            let ms = exec_time_ms(&mut conn, &q.query_text).await?;
            if i > 0 {
                best = best.min(ms); // discard the warm-up run
            }
        }
        out.insert(q.fingerprint.clone(), best);
    }
    Ok(out)
}

/// Combine baseline and candidate timing maps into a weighted impact summary.
///
/// `noise_floor_ms` guards against false regressions: a query whose baseline is
/// below the floor is too fast to time reliably (a fraction of a millisecond of
/// jitter reads as a huge relative swing), so it is excluded from the
/// worst-regression calculation. It still contributes to the weighted totals.
pub fn summarize(
    baseline: &HashMap<String, f64>,
    candidate: &HashMap<String, f64>,
    workload: &[WorkloadQuery],
    samples: usize,
    noise_floor_ms: f64,
) -> MeasuredImpact {
    let mut per_query = Vec::new();
    let mut base_total = 0.0;
    let mut cand_total = 0.0;
    let mut worst_regression_pct: f64 = 0.0;

    for q in workload {
        let b = *baseline.get(&q.fingerprint).unwrap_or(&0.0);
        let c = *candidate.get(&q.fingerprint).unwrap_or(&0.0);
        base_total += b * q.weight;
        cand_total += c * q.weight;
        let improvement_pct = if b > 0.0 { (b - c) / b * 100.0 } else { 0.0 };
        // A query vetoes a change only if it regressed by BOTH a meaningful
        // relative amount AND an absolute amount above the noise floor — so
        // sub-millisecond jitter on fast queries can never force a rollback.
        if b >= noise_floor_ms && (c - b) > noise_floor_ms {
            worst_regression_pct = worst_regression_pct.max(-improvement_pct);
        }
        per_query.push(QueryTiming {
            fingerprint: q.fingerprint.clone(),
            label: q.label.clone(),
            weight: q.weight,
            baseline_ms: b,
            candidate_ms: c,
            improvement_pct,
        });
    }

    let improvement_pct = if base_total > 0.0 {
        (base_total - cand_total) / base_total * 100.0
    } else {
        0.0
    };

    MeasuredImpact {
        improvement_pct,
        worst_regression_pct,
        baseline_ms: base_total,
        candidate_ms: cand_total,
        samples,
        per_query,
    }
}

async fn exec_time_ms(conn: &mut sqlx::PgConnection, sql: &str) -> anyhow::Result<f64> {
    // ANALYZE actually executes the query; our workload is read-only SELECTs.
    let row = sqlx::query(&format!("EXPLAIN (ANALYZE, TIMING, FORMAT JSON) {sql}"))
        .fetch_one(&mut *conn)
        .await?;
    let v: serde_json::Value = match row.try_get::<serde_json::Value, _>(0) {
        Ok(v) => v,
        Err(_) => {
            let s: String = row.try_get(0)?;
            serde_json::from_str(&s)?
        }
    };
    Ok(v.get(0)
        .and_then(|p| p.get("Execution Time"))
        .and_then(|t| t.as_f64())
        .unwrap_or(f64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wq(fp: &str, w: f64) -> WorkloadQuery {
        WorkloadQuery {
            fingerprint: fp.into(),
            query_text: String::new(),
            weight: w,
            label: None,
        }
    }

    #[test]
    fn summarize_is_weighted_and_tracks_regression() {
        let workload = vec![wq("a", 3.0), wq("b", 1.0)];
        let base: HashMap<_, _> = [("a".into(), 100.0), ("b".into(), 10.0)].into();
        // 'a' improves 50%, 'b' regresses 20%.
        let cand: HashMap<_, _> = [("a".into(), 50.0), ("b".into(), 12.0)].into();
        // Noise floor 0 => every query counts toward regression.
        let m = summarize(&base, &cand, &workload, 3, 0.0);
        // weighted: base=100*3+10=310, cand=50*3+12=162 -> ~47.7% improvement
        assert!((m.improvement_pct - (310.0 - 162.0) / 310.0 * 100.0).abs() < 1e-9);
        assert!((m.worst_regression_pct - 20.0).abs() < 1e-9);
    }

    #[test]
    fn noise_floor_excludes_tiny_queries_from_regression() {
        let workload = vec![wq("big", 1.0), wq("tiny", 1.0)];
        let base: HashMap<_, _> = [("big".into(), 100.0), ("tiny".into(), 0.3)].into();
        // 'big' improves; 'tiny' "regresses" 100% but is sub-floor jitter.
        let cand: HashMap<_, _> = [("big".into(), 60.0), ("tiny".into(), 0.6)].into();
        let m = summarize(&base, &cand, &workload, 3, 1.0);
        assert_eq!(m.worst_regression_pct, 0.0, "sub-floor query must not veto");
    }

    #[test]
    fn gate_requires_improvement_and_bounds_regression() {
        let good = MeasuredImpact {
            improvement_pct: 30.0,
            worst_regression_pct: 1.0,
            baseline_ms: 1.0,
            candidate_ms: 1.0,
            samples: 3,
            per_query: vec![],
        };
        assert!(good.passes(10.0, 5.0));
        let regressing = MeasuredImpact {
            worst_regression_pct: 9.0,
            ..good.clone()
        };
        assert!(!regressing.passes(10.0, 5.0));
        let weak = MeasuredImpact {
            improvement_pct: 2.0,
            ..good.clone()
        };
        assert!(!weak.passes(10.0, 5.0));
    }
}
