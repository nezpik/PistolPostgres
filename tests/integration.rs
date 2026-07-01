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
use pistol::{apply, catalog, db, demo, engine, reconcile, telemetry};

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

    // --- The audit log is genuinely append-only (migration 0002 trigger) ---
    let hid = history[0].id;
    let tamper =
        sqlx::query("UPDATE pistol.evolution_history SET rationale = 'tampered' WHERE id = $1")
            .bind(hid)
            .execute(&pool)
            .await;
    assert!(
        tamper.is_err(),
        "tampering with the audit log must be rejected"
    );

    // --- Measurement is read-only: a mutating workload entry can't change data ---
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS public.ro_probe(x int);
         TRUNCATE public.ro_probe;
         INSERT INTO public.ro_probe VALUES (1);",
    )
    .execute(&pool)
    .await
    .expect("ro_probe setup");
    let mutating = vec![catalog::WorkloadQuery {
        fingerprint: "mut".into(),
        query_text: "DELETE FROM public.ro_probe".into(),
        weight: 1.0,
        label: None,
        parameterized: false,
    }];
    let res = measure::measure(&pool, &mutating, 1).await;
    assert!(
        res.is_err(),
        "a mutating statement must be rejected under read-only measurement"
    );
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM public.ro_probe")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 1, "read-only measurement must not mutate data");

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

    // --- Automatic capture: self-drive from pg_stat_statements ---
    // The demo workload ran real queries, so pg_stat_statements has them.
    let n = telemetry::capture_workload(&pool, 1, 100)
        .await
        .expect("capture");
    assert!(
        n > 0,
        "expected to capture hot queries from pg_stat_statements"
    );
    let all = catalog::fetch_workload(&pool).await.unwrap();
    assert!(
        all.iter().any(|w| w.parameterized),
        "captured queries should be parameterized (normalized)"
    );
    // Parameterized (normalized) queries still yield index candidates, so the
    // proposal pipeline is fully self-driving.
    let caps = telemetry::candidates_validated(&pool, &all)
        .await
        .expect("candidates from captured workload");
    assert!(
        !caps.is_empty(),
        "captured parameterized queries should still yield candidates"
    );

    // --- Concrete-parameter sampling makes a captured query measurable ---
    let param = all
        .iter()
        .find(|w| w.parameterized)
        .expect("a captured parameterized query");
    let concrete = telemetry::concretize(&pool, param)
        .await
        .expect("concretize")
        .expect("captured predicate query should be concretizable");
    assert!(
        !concrete.contains('$'),
        "concretized SQL must not contain placeholders: {concrete}"
    );
    // The concretized query is now timeable with EXPLAIN (ANALYZE).
    let one = vec![catalog::WorkloadQuery {
        query_text: concrete,
        parameterized: false,
        ..param.clone()
    }];
    let timed = measure::measure(&pool, &one, 1)
        .await
        .expect("measure concretized captured query");
    assert!(
        timed.values().next().copied().unwrap_or(f64::MAX) < f64::MAX,
        "concretized captured query should produce a real timing"
    );

    // --- Crash reconciliation ---
    // (a) An orphan pistol index (built but never recorded — the crash-between-
    //     DDL-and-catalog case) is dropped.
    let orphan = IndexSpec::new("enrollments", vec![IndexColumn::asc("class_id")]);
    apply::build_index_online(&pool, &orphan)
        .await
        .expect("build orphan");
    assert!(apply::index_exists(&pool, "public", &orphan.index_name())
        .await
        .unwrap());
    let rep = reconcile::reconcile(&pool).await.expect("reconcile");
    assert!(
        !apply::index_exists(&pool, "public", &orphan.index_name())
            .await
            .unwrap(),
        "orphan pistol index must be dropped by reconcile"
    );
    assert!(
        rep.dropped_orphan
            .iter()
            .any(|s| s.ends_with(&orphan.index_name())),
        "reconcile report should list the dropped orphan"
    );

    // (b) A provenanced, physically-present index survives and stays in the
    //     rebuilt genome; if it's dropped out-of-band, reconcile marks it
    //     rolled_back and removes it from the genome.
    let genome = catalog::load_current_genome(&pool).await.unwrap();
    if let Some(applied) = genome.indexes.first().cloned() {
        assert!(
            apply::index_exists(&pool, &applied.schema, &applied.index_name())
                .await
                .unwrap(),
            "a genome index should physically exist after reconcile"
        );
        // Simulate an out-of-band drop, then reconcile.
        apply::drop_index_online(&pool, &applied)
            .await
            .expect("drop applied");
        reconcile::reconcile(&pool).await.expect("reconcile 2");
        let genome2 = catalog::load_current_genome(&pool).await.unwrap();
        assert!(
            !genome2
                .indexes
                .iter()
                .any(|i| i.index_name() == applied.index_name()),
            "an out-of-band-dropped index must be removed from the genome"
        );
    }
}

async fn count_public_indexes(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM pg_indexes WHERE schemaname = 'public'")
        .fetch_one(pool)
        .await
        .unwrap()
}
