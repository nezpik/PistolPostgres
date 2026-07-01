//! Orchestration (blueprint §4.6). One `run_once` executes the full evolution
//! cycle with a TWO-TIER gate:
//!   Tier 1 (cheap, estimated, no impact): hypopg drives the evolutionary search
//!           and a predicted-cost policy pre-filter.
//!   Tier 2 (real, measured, reversible): the top candidate is validated by
//!           `EXPLAIN (ANALYZE)` latency — on a shadow replica if configured
//!           (zero production impact), else in-place with guaranteed rollback.
//! Everything is written to the `pistol.*` catalog, recording predicted vs
//! measured so the estimate-vs-reality gap is always visible.

use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;

use crate::apply;
use crate::catalog::{self, NewHistory};
use crate::config::Config;
use crate::evaluator::Evaluator;
use crate::measure;
use crate::proposer::{ProposalContext, Proposer};
use crate::{db, policy, telemetry};

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

    // 3. Tier 1: evaluate baseline once, then run the evolutionary search
    //    (ranked on hypopg's ESTIMATED cost — cheap, no production impact).
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
            "    {}. fitness {:+.3} | est +{:.1}% cost | {}",
            i + 1,
            p.fitness,
            eval.predicted_improvement_pct,
            p.index.signature()
        );
    }

    // 4. Take the single best proposal ("one precise shot").
    let mut best = proposals.into_iter().next().unwrap();
    let now = Utc::now();
    // Include the index name so two proposals in the same millisecond can't
    // collide (insert_proposal upserts on the primary key).
    best.id = format!(
        "evo-{}-{}-{}",
        now.format("%Y%m%d"),
        now.timestamp_millis(),
        best.index.index_name()
    );
    let eval = best.evaluation.clone().unwrap();

    catalog::insert_proposal(
        pool,
        &best.id,
        &best.change_type,
        &best.target_object,
        &serde_json::to_value(&best)?,
    )
    .await?;

    // 5. Policy pre-filter (on the predicted estimate).
    let overrides = catalog::load_policy_overrides(pool).await?;
    let merged = policy::apply_overrides(config.policy.clone(), &overrides);
    let storage_today = storage_added_today_mb(pool).await?;
    let decision = policy::decide(&best.index, &eval, &genome, &merged, storage_today);

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
            "PASS (pre-filter)"
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

    // 6. Idempotency guard.
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

    // 7. Tier 2: measured trial (the decision-maker).
    let trial = measured_trial(pool, config, &best.index, &tele.workload).await?;

    // 8. Record provenance (predicted + measured).
    let real_size = if trial.kept {
        apply::index_size_bytes(pool, &best.index)
            .await
            .unwrap_or(0)
    } else {
        0
    };
    let actual_impact = json!({
        "predicted_improvement_pct": eval.predicted_improvement_pct,
        "measured": trial.impact,
        "measured_on": trial.measured_on,
        "kept": trial.kept,
        "real_index_size_bytes": real_size,
        "note": trial.note,
    });

    // Record provenance and (if kept) update the genome. If any of these writes
    // fail AFTER the index was applied, compensate by dropping the index so the
    // physical state can't diverge from an incomplete catalog (which the
    // idempotency guard would otherwise mask).
    let persist: anyhow::Result<i64> = async {
        let history_id = catalog::insert_history(
            pool,
            &NewHistory {
                proposal_id: best.id.clone(),
                change_type: best.change_type.clone(),
                target_object: best.target_object.clone(),
                ddl_executed: best.ddl.clone(),
                rationale: best.rationale.clone(),
                before_metrics: serde_json::to_value(&eval)?,
                after_metrics: actual_impact.clone(),
                actual_impact: actual_impact.clone(),
                rollback_ddl: best.index.drop_ddl(true),
                triggered_by: "auto".into(),
                genome_context: serde_json::to_value(&genome.indexes)?,
            },
        )
        .await?;

        if !trial.kept {
            catalog::mark_history_rolledback(pool, history_id, &actual_impact).await?;
            catalog::link_proposal_history(pool, &best.id, history_id, "rolled_back").await?;
        } else {
            catalog::link_proposal_history(pool, &best.id, history_id, "applied").await?;
            let mut new_genome = genome.clone();
            new_genome.indexes.push(best.index.clone());
            catalog::save_current_genome(
                pool,
                &new_genome,
                &json!({
                    "last_change": best.index.signature(),
                    "measured_improvement_pct": trial.impact.as_ref().map(|m| m.improvement_pct),
                    "predicted_improvement_pct": eval.predicted_improvement_pct,
                    "fitness": best.fitness,
                }),
            )
            .await?;
        }
        Ok(history_id)
    }
    .await;

    let history_id = match persist {
        Ok(id) => id,
        Err(e) => {
            if trial.kept {
                let _ = apply::drop_index_online(pool, &best.index).await;
                tracing::error!(
                    index = %best.index.index_name(),
                    error = %e,
                    "post-apply catalog write failed; dropped the applied index to stay consistent"
                );
            }
            return Err(e);
        }
    };

    if !trial.kept {
        return Ok(CycleReport {
            proposals: 1,
            applied: None,
            rolled_back: true,
            message: format!("{}: {}", best.index.index_name(), trial.note),
        });
    }

    let measured = match &trial.impact {
        Some(m) => format!("{:+.1}% measured latency", m.improvement_pct),
        None => format!("kept ({})", trial.measured_on),
    };
    println!(
        "✓ applied {} (history #{}) — {}",
        best.index.index_name(),
        history_id,
        measured
    );

    Ok(CycleReport {
        proposals: 1,
        applied: Some(best.index.index_name()),
        rolled_back: false,
        message: format!("applied index — {measured}"),
    })
}

struct Trial {
    impact: Option<measure::MeasuredImpact>,
    kept: bool,
    measured_on: &'static str,
    note: String,
}

/// Build the candidate, measure real latency, and decide keep-or-rollback.
/// Uses the shadow replica when configured (zero production impact); otherwise
/// an in-place trial on the primary that always ends in a definitive
/// keep-or-rollback.
async fn measured_trial(
    pool: &PgPool,
    config: &Config,
    index: &crate::genome::IndexSpec,
    workload: &[catalog::WorkloadQuery],
) -> anyhow::Result<Trial> {
    let mcfg = &config.measure;

    if !mcfg.enabled {
        apply::build_index_online(pool, index).await?;
        return Ok(Trial {
            impact: None,
            kept: true,
            measured_on: "none",
            note: "measured gate disabled; applied on predicted cost".into(),
        });
    }

    // Only concrete queries can be timed with EXPLAIN (ANALYZE); parameterized
    // (captured) queries were already gated on estimated GENERIC_PLAN cost.
    let concrete: Vec<catalog::WorkloadQuery> = workload
        .iter()
        .filter(|w| !w.parameterized)
        .cloned()
        .collect();
    if concrete.is_empty() {
        // Parameterized-only workload (e.g. fully auto-captured): apply on the
        // estimated gate that already passed. Restoring measured validation here
        // (via concrete parameter sampling) is the next follow-on.
        apply::build_index_online(pool, index).await?;
        return Ok(Trial {
            impact: None,
            kept: true,
            measured_on: "estimated-only",
            note: "parameterized-only workload; validated by estimated generic-plan cost".into(),
        });
    }

    let shadow = if mcfg.shadow_database_url.is_empty() {
        None
    } else {
        Some(db::connect(&mcfg.shadow_database_url).await?)
    };
    let target = shadow.as_ref().unwrap_or(pool);
    let where_ = if shadow.is_some() {
        "shadow replica"
    } else {
        "primary (in-place)"
    };
    println!(
        "▸ measured trial on {where_}: EXPLAIN (ANALYZE) × {} samples…",
        mcfg.samples
    );

    let baseline = measure::measure(target, &concrete, mcfg.samples).await?;
    apply::build_index_online(target, index).await?;
    // The trial index now exists on `target`; ensure it is removed on any error
    // path below, not just the normal keep/reject branches.
    let candidate = match measure::measure(target, &concrete, mcfg.samples).await {
        Ok(c) => c,
        Err(e) => {
            let _ = apply::drop_index_online(target, index).await;
            return Err(e);
        }
    };
    let impact = measure::summarize(
        &baseline,
        &candidate,
        &concrete,
        mcfg.samples,
        mcfg.noise_floor_ms,
    );
    let passed = impact.passes(
        mcfg.min_measured_improvement_pct,
        mcfg.max_measured_regression_pct,
    );
    println!(
        "▸ measured: {:+.1}% weighted latency, worst regression {:.1}% — {}",
        impact.improvement_pct,
        impact.worst_regression_pct,
        if passed { "PASS" } else { "FAIL" }
    );

    match shadow {
        Some(sp) => {
            // Never leave the trial index on the replica; surface (don't ignore)
            // a failed cleanup so a stray replica index is visible.
            if let Err(e) = apply::drop_index_online(&sp, index).await {
                tracing::warn!(
                    index = %index.index_name(),
                    error = %e,
                    "failed to drop trial index on shadow replica"
                );
            }
            if passed {
                apply::build_index_online(pool, index).await?;
                Ok(Trial {
                    impact: Some(impact),
                    kept: true,
                    measured_on: "shadow",
                    note: "validated on shadow replica, applied to primary".into(),
                })
            } else {
                Ok(Trial {
                    impact: Some(impact),
                    kept: false,
                    measured_on: "shadow",
                    note: "rejected by measured gate on shadow (no primary change)".into(),
                })
            }
        }
        None => {
            if passed {
                Ok(Trial {
                    impact: Some(impact),
                    kept: true,
                    measured_on: "primary",
                    note: "applied in-place; measured gate passed".into(),
                })
            } else {
                apply::drop_index_online(pool, index).await?;
                Ok(Trial {
                    impact: Some(impact),
                    kept: false,
                    measured_on: "primary",
                    note: "auto-rolled-back in-place: measured gate failed".into(),
                })
            }
        }
    }
}

async fn storage_added_today_mb(pool: &PgPool) -> anyhow::Result<f64> {
    let mb: Option<f64> = sqlx::query_scalar(
        "SELECT COALESCE(SUM((actual_impact->>'real_index_size_bytes')::bigint), 0)::float8
                / (1024*1024)
           FROM pistol.evolution_history
          WHERE status = 'applied' AND applied_at::date = now()::date",
    )
    .fetch_one(pool)
    .await?; // fail closed: never bypass the storage budget on a read error
    Ok(mb.unwrap_or(0.0))
}
