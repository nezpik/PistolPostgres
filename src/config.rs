//! Configuration & policy loading. Values come from a TOML file, with a few
//! high-value overrides available via environment variables.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database_url: String,
    #[serde(default)]
    pub evolution: Evolution,
    #[serde(default)]
    pub fitness: Fitness,
    #[serde(default)]
    pub policy: PolicyConfig,
    #[serde(default)]
    pub measure: Measure,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Measure {
    /// Tier-2 measured gate. When true, the top candidate is validated by real
    /// `EXPLAIN (ANALYZE)` latency (not just hypopg's estimated cost) before it
    /// is kept.
    pub enabled: bool,
    /// Measured samples per query (a warm-up run is taken and discarded on top).
    pub samples: usize,
    /// Keep the change only if measured weighted latency drops at least this %.
    pub min_measured_improvement_pct: f64,
    /// Roll back if any query's measured latency worsens beyond this %.
    pub max_measured_regression_pct: f64,
    /// Queries whose baseline is below this many milliseconds are too fast to
    /// time reliably and cannot veto a change (noise-floor guard).
    pub noise_floor_ms: f64,
    /// Optional replica/branch to measure against for ZERO production impact.
    /// If empty, the trial runs in-place on the primary and auto-rolls-back on
    /// a failed measured gate.
    pub shadow_database_url: String,
}

impl Default for Measure {
    fn default() -> Self {
        Self {
            enabled: true,
            samples: 5,
            min_measured_improvement_pct: 10.0,
            // In-place trials perturb the buffer cache (the index build evicts
            // pages), so unrelated queries can read a bit slower in the post-
            // build window. A tolerant default catches only egregious (plan-
            // flip) regressions; measure on a shadow replica to tighten this.
            max_measured_regression_pct: 25.0,
            noise_floor_ms: 1.0,
            shadow_database_url: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Evolution {
    /// 0 (or absent) => seed from OS entropy (real exploration).
    /// Any non-zero value => fully reproducible search (tests & demos).
    pub seed: u64,
    pub population_size: usize,
    pub generations: usize,
    pub mutation_rate: f64,
    pub max_columns_per_index: usize,
    pub min_column_support: f64,
}

impl Default for Evolution {
    fn default() -> Self {
        Self {
            seed: 42,
            population_size: 8,
            generations: 6,
            mutation_rate: 0.6,
            max_columns_per_index: 3,
            min_column_support: 0.05,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Fitness {
    pub w_cost: f64,
    pub w_storage: f64,
    pub w_write_amp: f64,
    pub w_redundancy: f64,
}

impl Default for Fitness {
    fn default() -> Self {
        Self {
            w_cost: 1.0,
            w_storage: 0.15,
            w_write_amp: 0.25,
            w_redundancy: 0.5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PolicyConfig {
    pub autonomy_level: String,
    pub min_predicted_improvement_pct: f64,
    pub max_regression_pct: f64,
    pub max_indexes_per_table: usize,
    pub max_storage_mb_per_day: f64,
    pub protected_schemas: Vec<String>,
    pub protected_tables: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            autonomy_level: "auto_safe".to_string(),
            min_predicted_improvement_pct: 15.0,
            max_regression_pct: 2.0,
            max_indexes_per_table: 8,
            max_storage_mb_per_day: 2048.0,
            protected_schemas: vec![
                "auth".into(),
                "storage".into(),
                "pg_catalog".into(),
                "information_schema".into(),
                "pistol".into(),
            ],
            protected_tables: vec![],
        }
    }
}

impl Config {
    /// Load config from `path` (default `pistol.toml`). Missing file is allowed
    /// as long as a database URL is supplied via the environment.
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let path = path.unwrap_or("pistol.toml");
        let mut cfg = if std::path::Path::new(path).exists() {
            let text = std::fs::read_to_string(path)?;
            toml::from_str::<Config>(&text)?
        } else {
            Config {
                database_url: String::new(),
                evolution: Evolution::default(),
                fitness: Fitness::default(),
                policy: PolicyConfig::default(),
                measure: Measure::default(),
            }
        };

        if let Ok(url) =
            std::env::var("PISTOL_DATABASE_URL").or_else(|_| std::env::var("DATABASE_URL"))
        {
            cfg.database_url = url;
        }
        if let Ok(seed) = std::env::var("PISTOL_SEED") {
            cfg.evolution.seed = seed.parse().unwrap_or(cfg.evolution.seed);
        }
        if let Ok(level) = std::env::var("PISTOL_AUTONOMY") {
            cfg.policy.autonomy_level = level;
        }
        if let Ok(url) = std::env::var("PISTOL_SHADOW_DATABASE_URL") {
            cfg.measure.shadow_database_url = url;
        }

        if cfg.database_url.is_empty() {
            anyhow::bail!("no database_url: set it in {path} or via PISTOL_DATABASE_URL");
        }
        Ok(cfg)
    }
}
