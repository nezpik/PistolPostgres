//! Policy & Decision Engine (blueprint §4.4, §5). Declarative safety gates plus
//! graduated autonomy. Nothing here mutates the database — it only decides
//! whether a fully-evaluated proposal is allowed to be applied.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

use crate::config::PolicyConfig;
use crate::evaluator::Evaluation;
use crate::genome::{Genome, IndexSpec};

#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    /// True if every hard safety gate passed.
    pub pass_gates: bool,
    /// True if the change should actually be applied (gates passed AND autonomy
    /// level permits automatic application).
    pub apply: bool,
    pub autonomy_level: String,
    pub reasons: Vec<String>,
}

/// Overlay `pistol.policies` rows onto the file/env config.
pub fn apply_overrides(mut cfg: PolicyConfig, overrides: &HashMap<String, Value>) -> PolicyConfig {
    if let Some(v) = overrides.get("autonomy_level").and_then(|v| v.as_str()) {
        cfg.autonomy_level = v.to_string();
    }
    if let Some(v) = overrides
        .get("min_predicted_improvement_pct")
        .and_then(|v| v.as_f64())
    {
        cfg.min_predicted_improvement_pct = v;
    }
    if let Some(v) = overrides.get("max_regression_pct").and_then(|v| v.as_f64()) {
        cfg.max_regression_pct = v;
    }
    if let Some(v) = overrides
        .get("max_indexes_per_table")
        .and_then(|v| v.as_u64())
    {
        cfg.max_indexes_per_table = v as usize;
    }
    if let Some(v) = overrides
        .get("max_storage_mb_per_day")
        .and_then(|v| v.as_f64())
    {
        cfg.max_storage_mb_per_day = v;
    }
    if let Some(v) = overrides.get("protected_tables").and_then(|v| v.as_array()) {
        cfg.protected_tables = v
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect();
    }
    if let Some(v) = overrides
        .get("protected_schemas")
        .and_then(|v| v.as_array())
    {
        cfg.protected_schemas = v
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect();
    }
    cfg
}

pub fn decide(
    index: &IndexSpec,
    eval: &Evaluation,
    genome: &Genome,
    cfg: &PolicyConfig,
    storage_added_today_mb: f64,
) -> Decision {
    let mut reasons = Vec::new();
    let mut pass = true;

    if is_protected(index, cfg) {
        pass = false;
        reasons.push(format!(
            "protected object {}.{} — never auto-modified",
            index.schema, index.table
        ));
    }

    if genome.contains(index) {
        pass = false;
        reasons.push("identical index already present in genome".into());
    }

    if eval.predicted_improvement_pct < cfg.min_predicted_improvement_pct {
        pass = false;
        reasons.push(format!(
            "predicted improvement {:.1}% < required {:.1}%",
            eval.predicted_improvement_pct, cfg.min_predicted_improvement_pct
        ));
    }

    if eval.worst_regression_pct > cfg.max_regression_pct {
        pass = false;
        reasons.push(format!(
            "worst-query regression {:.1}% > allowed {:.1}%",
            eval.worst_regression_pct, cfg.max_regression_pct
        ));
    }

    if genome.index_count_on(&index.schema, &index.table) >= cfg.max_indexes_per_table {
        pass = false;
        reasons.push(format!(
            "table already at max_indexes_per_table ({})",
            cfg.max_indexes_per_table
        ));
    }

    let storage_mb = eval.storage_bytes as f64 / (1024.0 * 1024.0);
    if storage_added_today_mb + storage_mb > cfg.max_storage_mb_per_day {
        pass = false;
        reasons.push(format!(
            "daily storage budget exceeded: {:.1} + {:.1} MB > {:.1} MB",
            storage_added_today_mb, storage_mb, cfg.max_storage_mb_per_day
        ));
    }

    let apply = match cfg.autonomy_level.as_str() {
        "advisory" => {
            reasons.push("advisory mode: proposal recorded, not applied".into());
            false
        }
        "auto_safe" | "auto_broad" => pass,
        other => {
            reasons.push(format!(
                "unknown autonomy_level '{other}': treating as advisory"
            ));
            false
        }
    };

    if pass && reasons.is_empty() {
        reasons.push("all gates passed".into());
    }

    Decision {
        pass_gates: pass,
        apply,
        autonomy_level: cfg.autonomy_level.clone(),
        reasons,
    }
}

fn is_protected(index: &IndexSpec, cfg: &PolicyConfig) -> bool {
    cfg.protected_schemas.iter().any(|s| s == &index.schema)
        || cfg
            .protected_tables
            .iter()
            .any(|t| t == &index.table || t == &format!("{}.{}", index.schema, index.table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evaluator::Evaluation;
    use crate::genome::{IndexColumn, IndexSpec};

    fn eval(improvement: f64, regression: f64, storage_mb: f64) -> Evaluation {
        Evaluation {
            predicted_improvement_pct: improvement,
            worst_regression_pct: regression,
            storage_bytes: (storage_mb * 1024.0 * 1024.0) as i64,
            baseline_cost: 100.0,
            candidate_cost: 50.0,
            per_query: vec![],
        }
    }

    fn cfg() -> PolicyConfig {
        PolicyConfig::default()
    }

    fn idx() -> IndexSpec {
        IndexSpec::new("student_progress", vec![IndexColumn::asc("student_id")])
    }

    #[test]
    fn approves_a_clearly_beneficial_safe_change() {
        let d = decide(
            &idx(),
            &eval(40.0, 0.0, 5.0),
            &crate::genome::Genome::default(),
            &cfg(),
            0.0,
        );
        assert!(d.pass_gates && d.apply);
    }

    #[test]
    fn rejects_below_min_improvement() {
        let d = decide(
            &idx(),
            &eval(5.0, 0.0, 5.0),
            &crate::genome::Genome::default(),
            &cfg(),
            0.0,
        );
        assert!(!d.pass_gates && !d.apply);
    }

    #[test]
    fn rejects_on_regression() {
        let d = decide(
            &idx(),
            &eval(40.0, 10.0, 5.0),
            &crate::genome::Genome::default(),
            &cfg(),
            0.0,
        );
        assert!(!d.pass_gates);
    }

    #[test]
    fn advisory_mode_holds_even_when_gates_pass() {
        let mut c = cfg();
        c.autonomy_level = "advisory".into();
        let d = decide(
            &idx(),
            &eval(40.0, 0.0, 5.0),
            &crate::genome::Genome::default(),
            &c,
            0.0,
        );
        assert!(d.pass_gates && !d.apply);
    }

    #[test]
    fn protected_schema_is_never_applied() {
        let mut c = cfg();
        c.protected_schemas = vec!["public".into()];
        let d = decide(
            &idx(),
            &eval(40.0, 0.0, 5.0),
            &crate::genome::Genome::default(),
            &c,
            0.0,
        );
        assert!(!d.pass_gates);
    }

    #[test]
    fn daily_storage_budget_is_enforced() {
        let mut c = cfg();
        c.max_storage_mb_per_day = 10.0;
        let d = decide(
            &idx(),
            &eval(40.0, 0.0, 8.0),
            &crate::genome::Genome::default(),
            &c,
            5.0,
        );
        assert!(!d.pass_gates); // 5 already used + 8 > 10
    }

    #[test]
    fn overrides_merge_from_catalog_rows() {
        let mut ov = std::collections::HashMap::new();
        ov.insert(
            "min_predicted_improvement_pct".to_string(),
            serde_json::json!(50.0),
        );
        let merged = apply_overrides(cfg(), &ov);
        assert_eq!(merged.min_predicted_improvement_pct, 50.0);
    }
}
