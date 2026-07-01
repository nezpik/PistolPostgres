//! Command-line surface. These subcommands are also the natural "Hermes tool"
//! seam described in blueprint §6 (propose_evolution, get_evolution_history…).

use clap::{Args, Parser, Subcommand};
use sqlx::{PgPool, Row};

use crate::config::Config;
use crate::evaluator::Evaluator;
use crate::proposer::{ProposalContext, Proposer};
use crate::{apply, catalog, db, demo, engine, telemetry};

#[derive(Parser)]
#[command(
    name = "pistol",
    version,
    about = "PistolPostgres — controlled evolutionary self-optimization on Postgres"
)]
pub struct Cli {
    /// Path to the config file.
    #[arg(long, global = true, default_value = "pistol.toml")]
    pub config: String,
    /// Override the database URL.
    #[arg(long, global = true)]
    pub database_url: Option<String>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create the pistol.* evolution catalog (runs migrations).
    Init,
    /// Demo helpers: build schema, seed data, load the workload.
    #[command(subcommand)]
    Demo(DemoCmd),
    /// Capture the real workload from pg_stat_statements into pistol.workload.
    Capture(CaptureArgs),
    /// Collect a telemetry snapshot and show derived index candidates.
    Collect,
    /// Run the evolutionary search and print ranked proposals (no changes).
    Propose,
    /// Run the full evolution cycle (telemetry→propose→gate→apply→monitor).
    Run(RunArgs),
    /// Show the active genome and evolution status.
    Status,
    /// Show the evolution history (applied & rolled-back changes).
    History(HistoryArgs),
    /// Roll back a previously applied change by its history id.
    Rollback(RollbackArgs),
}

#[derive(Subcommand)]
pub enum DemoCmd {
    /// Create schema, seed data, and load the workload in one shot.
    All(DemoAllArgs),
    /// Create the demo schema only.
    Schema,
    /// Seed synthetic data only.
    Seed,
    /// Register and exercise the representative workload only.
    Load(DemoAllArgs),
}

#[derive(Args)]
pub struct DemoAllArgs {
    /// Workload exercise iterations.
    #[arg(long, default_value_t = 20)]
    pub iterations: u32,
}

#[derive(Args)]
pub struct CaptureArgs {
    /// Only capture queries called at least this many times.
    #[arg(long, default_value_t = 5)]
    pub min_calls: i64,
    /// Max number of (hottest) queries to capture.
    #[arg(long, default_value_t = 50)]
    pub limit: i64,
}

#[derive(Args)]
pub struct RunArgs {
    /// Keep running on an interval instead of a single cycle.
    #[arg(long)]
    pub watch: bool,
    /// Interval between cycles in --watch mode (seconds).
    #[arg(long, default_value_t = 300)]
    pub interval: u64,
}

#[derive(Args)]
pub struct HistoryArgs {
    #[arg(long, default_value_t = 20)]
    pub limit: i64,
}

#[derive(Args)]
pub struct RollbackArgs {
    /// evolution_history id to roll back.
    pub id: i64,
}

pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    let mut config = Config::load(Some(&cli.config))?;
    if let Some(url) = &cli.database_url {
        config.database_url = url.clone();
    }
    let pool = db::connect(&config.database_url).await?;

    match cli.command {
        Command::Init => {
            db::migrate(&pool).await?;
            println!("✓ evolution catalog ready (schema pistol.*)");
        }
        Command::Demo(cmd) => match cmd {
            DemoCmd::All(a) => {
                // load() writes to pistol.workload, so the catalog must exist.
                db::migrate(&pool).await?;
                demo::schema(&pool).await?;
                demo::seed(&pool).await?;
                demo::load(&pool, a.iterations).await?;
            }
            DemoCmd::Schema => demo::schema(&pool).await?,
            DemoCmd::Seed => demo::seed(&pool).await?,
            DemoCmd::Load(a) => {
                db::migrate(&pool).await?;
                demo::load(&pool, a.iterations).await?;
            }
        },
        Command::Capture(a) => capture(&pool, a).await?,
        Command::Collect => collect(&pool).await?,
        Command::Propose => propose(&pool, &config).await?,
        Command::Run(a) => run(&pool, &config, a).await?,
        Command::Status => status(&pool).await?,
        Command::History(a) => history(&pool, a.limit).await?,
        Command::Rollback(a) => rollback(&pool, a.id).await?,
    }
    Ok(())
}

async fn capture(pool: &PgPool, args: CaptureArgs) -> anyhow::Result<()> {
    let n = telemetry::capture_workload(pool, args.min_calls, args.limit).await?;
    println!(
        "✓ captured {n} workload quer{} from pg_stat_statements (min_calls={}, limit={})",
        if n == 1 { "y" } else { "ies" },
        args.min_calls,
        args.limit
    );
    if n > 0 {
        println!("  run `pistol collect` to see derived candidates, or `pistol run` to evolve.");
    } else {
        println!("  nothing captured yet — exercise your app so pg_stat_statements has data.");
    }
    Ok(())
}

async fn collect(pool: &PgPool) -> anyhow::Result<()> {
    let tele = telemetry::collect(pool).await?;
    println!(
        "telemetry snapshot #{}: {} workload queries, {} tables",
        tele.snapshot_id,
        tele.workload.len(),
        tele.table_stats.len()
    );
    let candidates = telemetry::candidates_validated(pool, &tele.workload).await?;
    println!("\nindex candidates derived from the workload:");
    for c in &candidates {
        println!(
            "  {}.{}  support={:.0}  eq={:?}  sort={:?}",
            c.schema, c.table, c.support, c.eq_columns, c.sort_columns
        );
    }
    Ok(())
}

async fn propose(pool: &PgPool, config: &Config) -> anyhow::Result<()> {
    let tele = telemetry::collect(pool).await?;
    if tele.workload.is_empty() {
        println!("no workload registered; run `pistol demo load` first");
        return Ok(());
    }
    let candidates = telemetry::candidates_validated(pool, &tele.workload).await?;
    let genome = catalog::load_current_genome(pool).await?;
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
        println!("no beneficial proposals found");
        return Ok(());
    }
    println!("ranked proposals (fitness | predicted cost reduction | index):");
    for (i, p) in proposals.iter().enumerate() {
        let e = p.evaluation.as_ref().unwrap();
        println!(
            "  {}. {:+.3} | +{:.1}% | {}\n       {}",
            i + 1,
            p.fitness,
            e.predicted_improvement_pct,
            p.index.signature(),
            p.rationale
        );
    }
    Ok(())
}

async fn run(pool: &PgPool, config: &Config, args: RunArgs) -> anyhow::Result<()> {
    if !args.watch {
        let r = engine::run_once(pool, config).await?;
        println!("\n— {}", r.message);
        return Ok(());
    }
    println!(
        "watch mode: cycle every {}s (Ctrl-C to stop)",
        args.interval
    );
    loop {
        match engine::run_once(pool, config).await {
            Ok(r) => println!("— {}", r.message),
            Err(e) => tracing::error!("cycle failed: {e:#}"),
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(args.interval)) => {}
            _ = tokio::signal::ctrl_c() => { println!("\nstopping watch loop"); break; }
        }
    }
    Ok(())
}

async fn status(pool: &PgPool) -> anyhow::Result<()> {
    let genome = catalog::load_current_genome(pool).await?;
    println!(
        "active genome: {} pistol-managed index(es)",
        genome.indexes.len()
    );
    for i in &genome.indexes {
        println!("  • {}", i.signature());
    }
    let rows = sqlx::query(
        "SELECT status, count(*) AS n FROM pistol.proposals GROUP BY status ORDER BY status",
    )
    .fetch_all(pool)
    .await?;
    if !rows.is_empty() {
        println!("\nproposals by status:");
        for r in rows {
            println!(
                "  {:<12} {}",
                r.get::<String, _>("status"),
                r.get::<i64, _>("n")
            );
        }
    }
    let applied: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pistol.evolution_history WHERE status='applied'")
            .fetch_one(pool)
            .await?;
    let rb: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pistol.evolution_history WHERE status='rolled_back'",
    )
    .fetch_one(pool)
    .await?;
    println!("\nhistory: {applied} applied, {rb} rolled back");
    Ok(())
}

async fn history(pool: &PgPool, limit: i64) -> anyhow::Result<()> {
    let rows = catalog::fetch_history(pool, limit).await?;
    if rows.is_empty() {
        println!("no evolution history yet");
        return Ok(());
    }
    for h in rows {
        // Prefer the real measured improvement; fall back to the prediction,
        // then to n/a. (Matches the nested `measured` shape the engine writes.)
        let actual = h
            .actual_impact
            .as_ref()
            .and_then(|v| {
                v.get("measured")
                    .and_then(|m| m.get("improvement_pct"))
                    .and_then(|x| x.as_f64())
                    .map(|x| format!("{x:+.1}% measured"))
                    .or_else(|| {
                        v.get("predicted_improvement_pct")
                            .and_then(|x| x.as_f64())
                            .map(|x| format!("{x:+.1}% predicted"))
                    })
            })
            .unwrap_or_else(|| "n/a".into());
        println!(
            "#{:<4} {}  [{}]  {} {}  actual={}",
            h.id,
            h.applied_at.format("%Y-%m-%d %H:%M:%S"),
            h.status,
            h.change_type,
            h.target_object.unwrap_or_default(),
            actual
        );
        if let Some(d) = &h.ddl_executed {
            println!("      ddl:      {d}");
        }
        if let Some(r) = &h.rollback_ddl {
            println!("      rollback: {r}");
        }
        if let Some(rat) = &h.rationale {
            println!("      why:      {rat}");
        }
    }
    Ok(())
}

async fn rollback(pool: &PgPool, id: i64) -> anyhow::Result<()> {
    let h = match catalog::fetch_history_by_id(pool, id).await? {
        Some(h) => h,
        None => {
            println!("no history entry #{id}");
            return Ok(());
        }
    };
    if h.status == "rolled_back" {
        println!("#{id} is already rolled back");
        return Ok(());
    }
    let ddl = match &h.rollback_ddl {
        Some(d) if !d.is_empty() => d.clone(),
        _ => {
            println!("#{id} has no rollback DDL recorded");
            return Ok(());
        }
    };
    println!("rolling back #{id}: {ddl}");
    apply::execute_rollback(pool, &ddl).await?;
    let impact = serde_json::json!({ "manual_rollback": true });
    catalog::mark_history_rolledback(pool, id, &impact).await?;

    // Remove exactly the rolled-back index from the active genome. Compare on
    // the generated rollback DDL (exact identity) rather than substring-matching
    // the raw DDL, which could drop unrelated entries whose names overlap.
    let genome = catalog::load_current_genome(pool).await?;
    let kept: Vec<_> = genome
        .indexes
        .into_iter()
        .filter(|i| i.drop_ddl(true) != ddl)
        .collect();
    catalog::save_current_genome(
        pool,
        &crate::genome::Genome { indexes: kept },
        &serde_json::json!({ "manual_rollback_of": id }),
    )
    .await?;
    println!("✓ rolled back #{id}");
    Ok(())
}
