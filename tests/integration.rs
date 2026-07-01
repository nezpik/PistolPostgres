//! End-to-end integration test against a real Postgres (with hypopg +
//! pg_stat_statements). Gated on `PISTOL_TEST_DATABASE_URL` so `cargo test`
//! stays green without a database; CI and the demo Postgres provide one.
//!
//! Run with:
//!   PISTOL_TEST_DATABASE_URL=postgres://pistol@127.0.0.1:55432/pistol cargo test --test integration -- --nocapture

use pistol::config::{Config, Evolution, Fitness, Measure, PolicyConfig};
use pistol::evaluator::Evaluator;
use pistol::genome::{Genome, IndexColumn, IndexSpec};
use pistol::measure;
use pistol::proposer::evolutionary::EvolutionaryProposer;
use pistol::proposer::ProposalContext;
use pistol::{apply, catalog, db, demo, engine, telemetry};

fn test_config(url: &str) -> Config {
    Config {
        database_url: url.to_string(),
        evolution: Evolution {
            seed: 42,
            ..Evolution::default()
        },
        fitness: Fitness::default(),
        policy: PolicyConfig {
            autonomy_level: "auto_safe".into(),
            ..PolicyConfig::default()
        },
        measure: Measure::default(),
    }
}

#[tokio::test]
async fn end_to_end_evolution_cycle() {
    let url = match std::env::var("PISTOL_TEST_DATABASE_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skipping: PISTOL_TEST_DATABASE_URL not set");
            return;
        }
    };
    let config = test_config(&url);
    let pool = db::connect(&url).await.expect("connect");
    db::migrate(&pool).await.expect("migrate");

    // Clean slate so the run is deterministic.
    sqlx::raw_sql(
        "TRUNCATE pistol.proposals, pistol.evolution_history, pistol.current_genome,
                  pistol.telemetry_snapshots, pistol.workload, pistol.policies
         RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("truncate catalog");

    demo::schema(&pool).await.expect("schema");
    demo::seed(&pool).await.expect("seed");
    demo::load(&pool, 2).await.expect("load");

    // --- Candidate extraction is grounded in real columns ---
    let tele = telemetry::collect(&pool).await.expect("collect");
    let candidates = telemetry::candidates_validated(&pool, &tele.workload)
        .await
        .expect("candidates");
    assert!(
        !candidates.is_empty(),
        "expected workload-derived candidates"
    );

    // --- The seeded proposer is reproducible ---
    let evaluator = Evaluator::new(&pool, &tele.workload)
        .await
        .expect("evaluator");
    let genome = Genome::default();
    let top = |ps: &[pistol::proposer::Proposal]| ps.first().map(|p| p.index.signature());
    let ctx1 = ProposalContext {
        candidates: candidates.clone(),
        workload: &tele.workload,
        table_stats: &tele.table_stats,
        genome: &genome,
        config: &config,
    };
    let run1 = EvolutionaryProposer
        .propose(&evaluator, &ctx1)
        .await
        .expect("propose1");
    let ctx2 = ProposalContext {
        candidates: candidates.clone(),
        workload: &tele.workload,
        table_stats: &tele.table_stats,
        genome: &genome,
        config: &config,
    };
    let run2 = EvolutionaryProposer
        .propose(&evaluator, &ctx2)
        .await
        .expect("propose2");
    assert!(!run1.is_empty(), "expected at least one proposal");
    assert_eq!(
        top(&run1),
        top(&run2),
        "same seed must yield same top proposal"
    );

    // --- Direct measurement: a real index beats a seq scan (robust signal) ---
    // student_dashboard scans 150k rows and sorts without an index; an index on
    // (student_id, created_at) turns it into a tiny index scan — a large,
    // noise-proof latency win.
    let spec = IndexSpec::new(
        "student_progress",
        vec![
            IndexColumn::asc("student_id"),
            IndexColumn::desc("created_at"),
        ],
    );
    let base = measure::measure(&pool, &tele.workload, 2)
        .await
        .expect("baseline");
    apply::build_index_online(&pool, &spec)
        .await
        .expect("build");
    let cand = measure::measure(&pool, &tele.workload, 2)
        .await
        .expect("candidate");
    let impact = measure::summarize(
        &base,
        &cand,
        &tele.workload,
        2,
        config.measure.noise_floor_ms,
    );
    let dash = impact
        .per_query
        .iter()
        .find(|t| t.fingerprint == "student_dashboard")
        .expect("timing for student_dashboard");
    assert!(
        dash.improvement_pct > 0.0,
        "indexed dashboard query should be measurably faster (got {:.1}%)",
        dash.improvement_pct
    );
    apply::drop_index_online(&pool, &spec).await.expect("drop");

    // --- A full cycle applies an index validated by MEASURED latency ---
    let report = engine::run_once(&pool, &config).await.expect("run_once");
    assert!(
        report.applied.is_some(),
        "first cycle should apply a measured-good index"
    );
    let history = catalog::fetch_history(&pool, 10).await.expect("history");
    assert!(!history.is_empty());
    assert_eq!(history[0].status, "applied");
    // Provenance carries both the prediction and the real measurement.
    let ai = history[0].actual_impact.as_ref().unwrap();
    assert!(ai.get("measured").is_some(), "measured impact recorded");
    assert!(ai.get("predicted_improvement_pct").is_some());
    assert!(history[0]
        .rollback_ddl
        .as_ref()
        .unwrap()
        .contains("DROP INDEX"));

    // --- Measured gate auto-rolls-back when the bar can't be met ---
    // An impossibly high improvement bar forces the in-place trial to roll back
    // whichever candidate it picks.
    let indexes_before = count_public_indexes(&pool).await;
    let mut strict = config.clone();
    strict.measure.min_measured_improvement_pct = 99.999;
    let trial = engine::run_once(&pool, &strict).await.expect("strict run");
    assert!(
        trial.applied.is_none() && trial.rolled_back,
        "unmeetable measured bar must roll back, not keep"
    );
    assert_eq!(
        indexes_before,
        count_public_indexes(&pool).await,
        "a rolled-back trial must leave no index behind"
    );
}

async fn count_public_indexes(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'")
        .fetch_one(pool)
        .await
        .unwrap()
}
