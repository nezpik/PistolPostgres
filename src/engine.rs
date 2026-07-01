//! Orchestration (blueprint §4.6). One `run_once` executes the full evolution
//! cycle: telemetry → propose → evaluate → policy-gate → apply → monitor →
//! record. Everything it does is written to the `pistol.*` catalog.

use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;

use crate::apply;
use crate::catalog::{self, NewHistory};
use crate::config::Config;
use crate::evaluator::Evaluator;
use crate::policy;
use crate::proposer::{ProposalContext, Proposer};
use crate::telemetry;

#[allow(dead_code)] // fields consumed by tests and future API callers
pub struct CycleReport {
    pub proposals: usize,
    pub applied: Option<String>,
    pub rolled_back: bool,
    pub message: String,
}

pub async fn run_once(pool: &PgPool, config: &Config) -> anyhow::Result<CycleReport> {
    // 1. Telemetry.
    let tele = telemetry::collect(pool).await?;
    if tele.workload.is_empty() {
        return Ok(CycleReport {
            proposals: 0,
            applied: None,
            rolled_back: false,
            message: "no workload registered — run `pistol demo load` or populate pistol.workload"
                .into(),
        });
    }
    println!(
        "▸ telemetry snapshot #{} — {} workload queries, {} tables",
        tele.snapshot_id,
        tele.workload.len(),
        tele.table_stats.len()
    );

    // 2. Candidate columns from the workload (validated against real columns).
    let candidates = telemetry::candidates_validated(pool, &tele.workload).await?;
    let genome = catalog::load_current_genome(pool).await?;

    // 3. Evaluate baseline once, then run the evolutionary search.
    let evaluator = Evaluator::new(pool, &tele.workload).await?;
    let ctx = ProposalContext {
        candidates,
        workload: &tele.workload,
        table_stats: &tele.table_stats,
        genome: &genome,
        config,
    };
    let proposer = Proposer::from_env();
    let proposals = proposer.propose(&evaluator, &ctx).await?;

    if proposals.is_empty() {
        return Ok(CycleReport {
            proposals: 0,
            applied: None,
            rolled_back: false,
            message: "no beneficial, safe proposals found this cycle".into(),
        });
    }

    println!(
        "▸ {} candidate proposal(s) after evolutionary search:",
        proposals.len()
    );
    for (i, p) in proposals.iter().enumerate() {
        let eval = p.evaluation.as_ref().unwrap();
        println!(
            "    {}. fitness {:+.3} | +{:.1}% cost | {} ",
            i + 1,
            p.fitness,
            eval.predicted_improvement_pct,
            p.index.signature()
        );
    }

    // 4. Take the single best proposal ("one precise shot").
    let mut best = proposals.into_iter().next().unwrap();
    let now = Utc::now();
    best.id = format!("evo-{}-{}", now.format("%Y%m%d"), now.timestamp_millis());
    let eval = best.evaluation.clone().unwrap();

    catalog::insert_proposal(
        pool,
        &best.id,
        &best.change_type,
        &best.target_object,
        &serde_json::to_value(&best)?,
    )
    .await?;

    // 5. Policy gate.
    let overrides = catalog::load_policy_overrides(pool).await?;
    let merged = policy::apply_overrides(config.policy.clone(), &overrides);
    let storage_today = storage_added_today_mb(pool).await?;
    let decision = policy::decide(&best.index, &eval, &genome, &merged, storage_today);

    // "approved" covers both auto-apply and advisory-hold (gates passed);
    // "rejected" means a hard gate failed.
    let status = if decision.pass_gates {
        "approved"
    } else {
        "rejected"
    };
    catalog::set_proposal_decision(
        pool,
        &best.id,
        status,
        &serde_json::to_value(&eval)?,
        &serde_json::to_value(&decision)?,
    )
    .await?;

    println!(
        "▸ policy [{}]: {} — {}",
        decision.autonomy_level,
        if decision.apply {
            "APPLY"
        } else if decision.pass_gates {
            "hold (advisory)"
        } else {
            "reject"
        },
        decision.reasons.join("; ")
    );

    if !decision.apply {
        return Ok(CycleReport {
            proposals: 1,
            applied: None,
            rolled_back: false,
            message: format!(
                "best proposal {} not applied: {}",
                best.id,
                decision.reasons.join("; ")
            ),
        });
    }

    // 6. Idempotency guard, then apply + monitor.
    if apply::index_exists(pool, &best.index.schema, &best.index.index_name()).await? {
        return Ok(CycleReport {
            proposals: 1,
            applied: None,
            rolled_back: false,
            message: format!(
                "index {} already exists — skipping",
                best.index.index_name()
            ),
        });
    }

    println!("▸ applying online: {}", best.ddl);
    let outcome = apply::apply_and_monitor(
        pool,
        &best.index,
        &eval,
        &tele.workload,
        merged.max_regression_pct,
    )
    .await?;

    // 7. Record provenance (immutable history).
    let history_id = catalog::insert_history(
        pool,
        &NewHistory {
            proposal_id: best.id.clone(),
            change_type: best.change_type.clone(),
            target_object: best.target_object.clone(),
            ddl_executed: outcome.ddl_executed.clone(),
            rationale: best.rationale.clone(),
            before_metrics: serde_json::to_value(&eval)?,
            after_metrics: outcome.actual_impact.clone(),
            actual_impact: outcome.actual_impact.clone(),
            rollback_ddl: outcome.rollback_ddl.clone(),
            triggered_by: "auto".into(),
            genome_context: serde_json::to_value(&genome.indexes)?,
        },
    )
    .await?;

    if outcome.rolled_back {
        catalog::mark_history_rolledback(pool, history_id, &outcome.actual_impact).await?;
        catalog::link_proposal_history(pool, &best.id, history_id, "rolled_back").await?;
        return Ok(CycleReport {
            proposals: 1,
            applied: None,
            rolled_back: true,
            message: format!(
                "{} auto-rolled-back after post-apply regression",
                outcome.index_name
            ),
        });
    }

    // 8. Success — update the active genome.
    catalog::link_proposal_history(pool, &best.id, history_id, "applied").await?;
    let mut new_genome = genome.clone();
    new_genome.indexes.push(best.index.clone());
    catalog::save_current_genome(
        pool,
        &new_genome,
        &json!({
            "last_change": best.index.signature(),
            "predicted_improvement_pct": eval.predicted_improvement_pct,
            "fitness": best.fitness,
        }),
    )
    .await?;

    let actual = outcome
        .actual_impact
        .get("actual_improvement_pct")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    println!(
        "✓ applied {} (history #{}) — actual {:.1}% weighted plan-cost reduction",
        outcome.index_name, history_id, actual
    );

    Ok(CycleReport {
        proposals: 1,
        applied: Some(outcome.index_name),
        rolled_back: false,
        message: format!("applied index, actual improvement {actual:.1}%"),
    })
}

async fn storage_added_today_mb(pool: &PgPool) -> anyhow::Result<f64> {
    let mb: Option<f64> = sqlx::query_scalar(
        "SELECT COALESCE(SUM((actual_impact->>'real_index_size_bytes')::bigint), 0)::float8
                / (1024*1024)
           FROM pistol.evolution_history
          WHERE status = 'applied' AND applied_at::date = now()::date",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(Some(0.0));
    Ok(mb.unwrap_or(0.0))
}
