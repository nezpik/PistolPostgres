//! End-to-end integration test against a real Postgres (with hypopg +
//! pg_stat_statements). Gated on `PISTOL_TEST_DATABASE_URL` so `cargo test`
//! stays green without a database; CI and the demo Postgres provide one.
//!
//! Run with:
//!   PISTOL_TEST_DATABASE_URL=postgres://pistol@127.0.0.1:55432/pistol cargo test --test integration -- --nocapture

use pistol::config::{Config, Evolution, Fitness, PolicyConfig};
use pistol::evaluator::Evaluator;
use pistol::genome::{Genome, IndexColumn, IndexSpec};
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

    // --- A full cycle applies an index and records provenance ---
    let report = engine::run_once(&pool, &config).await.expect("run_once");
    assert!(
        report.applied.is_some(),
        "first cycle should apply an index"
    );
    let history = catalog::fetch_history(&pool, 10).await.expect("history");
    assert!(!history.is_empty());
    assert_eq!(history[0].status, "applied");
    assert!(history[0]
        .rollback_ddl
        .as_ref()
        .unwrap()
        .contains("DROP INDEX"));

    // --- Post-apply monitor auto-rolls-back on regression ---
    // A negative regression gate forces rollback of any real change, exercising
    // the monitor path deterministically.
    let spec = IndexSpec::new("enrollments", vec![IndexColumn::asc("student_id")]);
    let eval = evaluator.evaluate(&spec).await.expect("evaluate spec");
    let outcome = apply::apply_and_monitor(&pool, &spec, &eval, &tele.workload, -1.0)
        .await
        .expect("apply_and_monitor");
    assert!(
        outcome.rolled_back,
        "negative gate must trigger auto-rollback"
    );
    assert!(
        !apply::index_exists(&pool, "public", &spec.index_name())
            .await
            .unwrap(),
        "rolled-back index must not exist"
    );
}
