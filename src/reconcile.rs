//! Crash reconciliation (blueprint §5 "crash safety").
//!
//! The compensating writes in `engine` handle the *error* path, but a crash
//! (SIGKILL / power loss) between `CREATE INDEX CONCURRENTLY` and the catalog
//! write can still leave the physical schema and the `pistol.*` catalog out of
//! sync. This startup pass reconciles them, touching ONLY pistol-managed
//! (`pi_*`) indexes — never a user index:
//!
//!   - INVALID `pi_*` index  → drop (a crashed CONCURRENTLY build);
//!   - valid `pi_*` with applied provenance → keep;
//!   - valid `pi_*` without provenance → drop (unverified; the loop re-derives it);
//!   - applied-history index missing physically → mark rolled_back;
//!   - `current_genome` is rebuilt to the provenanced, physically-present set.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};
use sqlx::{PgPool, Row};

use crate::apply;
use crate::catalog;
use crate::genome::{Genome, IndexSpec};

#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub dropped_invalid: Vec<String>,
    pub dropped_orphan: Vec<String>,
    pub marked_missing: Vec<i64>,
    pub genome_indexes: usize,
}

impl ReconcileReport {
    pub fn is_clean(&self) -> bool {
        self.dropped_invalid.is_empty()
            && self.dropped_orphan.is_empty()
            && self.marked_missing.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum IndexAction {
    Keep,
    DropInvalid,
    DropOrphan,
}

/// Decide what to do with a physical pistol index given its validity and whether
/// the catalog expected it. Pure so it's unit-testable.
fn classify_index(valid: bool, expected: bool) -> IndexAction {
    if !valid {
        IndexAction::DropInvalid
    } else if expected {
        IndexAction::Keep
    } else {
        IndexAction::DropOrphan
    }
}

pub async fn reconcile(pool: &PgPool) -> anyhow::Result<ReconcileReport> {
    // Expected indexes = every applied history row, keyed by (schema, name),
    // carrying the originating proposal's IndexSpec (for genome rebuild).
    let hrows = sqlx::query(
        "SELECT h.id, h.target_object, h.ddl_executed, p.proposal_json
           FROM pistol.evolution_history h
           LEFT JOIN pistol.proposals p ON p.id = h.proposal_id
          WHERE h.status = 'applied'",
    )
    .fetch_all(pool)
    .await?;

    let mut expected: HashMap<(String, String), (i64, Option<IndexSpec>)> = HashMap::new();
    for r in hrows {
        let hid: i64 = r.get("id");
        let target: Option<String> = r.get("target_object");
        let ddl: Option<String> = r.get("ddl_executed");
        let schema = target
            .as_deref()
            .and_then(|t| t.split('.').next())
            .unwrap_or("public")
            .to_string();
        let name = match ddl.as_deref().and_then(extract_index_name) {
            Some(n) => n,
            None => continue,
        };
        let spec = r
            .get::<Option<Value>, _>("proposal_json")
            .and_then(|v| v.get("index").cloned())
            .and_then(|iv| serde_json::from_value::<IndexSpec>(iv).ok());
        expected.insert((schema, name), (hid, spec));
    }

    // Physical pistol-managed indexes and their validity.
    let prows = sqlx::query(
        "SELECT n.nspname AS schema, c.relname AS name, i.indisvalid AS valid
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE substr(c.relname, 1, 3) = 'pi_'",
    )
    .fetch_all(pool)
    .await?;

    let mut report = ReconcileReport::default();
    let mut present: HashSet<(String, String)> = HashSet::new();

    for r in prows {
        let schema: String = r.get("schema");
        let name: String = r.get("name");
        let valid: bool = r.get("valid");
        let key = (schema.clone(), name.clone());
        match classify_index(valid, expected.contains_key(&key)) {
            IndexAction::Keep => {
                present.insert(key);
            }
            IndexAction::DropInvalid => {
                apply::drop_index_by_name(pool, &schema, &name).await?;
                tracing::warn!(index = %name, "reconcile: dropped INVALID index (crashed build)");
                report.dropped_invalid.push(format!("{schema}.{name}"));
            }
            IndexAction::DropOrphan => {
                apply::drop_index_by_name(pool, &schema, &name).await?;
                tracing::warn!(index = %name, "reconcile: dropped ORPHAN pistol index (no applied provenance)");
                report.dropped_orphan.push(format!("{schema}.{name}"));
            }
        }
    }

    // Applied history whose index is no longer physically present → the audit
    // log claims an index that's gone (dropped out-of-band or crash). Mark it
    // rolled_back so provenance stays honest.
    for ((schema, name), (hid, _)) in &expected {
        if !present.contains(&(schema.clone(), name.clone())) {
            catalog::mark_history_rolledback(
                pool,
                *hid,
                &json!({ "reconciled": "index not present at startup" }),
            )
            .await?;
            tracing::warn!(history_id = hid, index = %name, "reconcile: applied index missing; marked rolled_back");
            report.marked_missing.push(*hid);
        }
    }

    // Rebuild the active genome to physically-present, provenanced indexes.
    let specs: Vec<IndexSpec> = expected
        .into_iter()
        .filter_map(|((schema, name), (_, spec))| {
            if present.contains(&(schema, name)) {
                spec
            } else {
                None
            }
        })
        .collect();
    report.genome_indexes = specs.len();

    let current = catalog::load_current_genome(pool).await.unwrap_or_default();
    let new_genome = Genome { indexes: specs };
    if !report.is_clean() || genome_names(&current) != genome_names(&new_genome) {
        catalog::save_current_genome(
            pool,
            &new_genome,
            &json!({ "reconciled_at": chrono::Utc::now() }),
        )
        .await?;
    }

    Ok(report)
}

fn genome_names(g: &Genome) -> Vec<String> {
    let mut v: Vec<String> = g
        .indexes
        .iter()
        .map(|i| format!("{}.{}", i.schema, i.index_name()))
        .collect();
    v.sort();
    v
}

/// Extract the index name from a `CREATE INDEX [CONCURRENTLY] <name> ON …` DDL.
fn extract_index_name(ddl: &str) -> Option<String> {
    let mut toks = ddl.split_whitespace();
    let mut seen_index = false;
    for t in toks.by_ref() {
        if seen_index {
            if t.eq_ignore_ascii_case("CONCURRENTLY") {
                continue;
            }
            return Some(t.trim_matches('"').to_string());
        }
        if t.eq_ignore_ascii_case("INDEX") {
            seen_index = true;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_covers_the_three_cases() {
        assert_eq!(classify_index(false, true), IndexAction::DropInvalid);
        assert_eq!(classify_index(false, false), IndexAction::DropInvalid);
        assert_eq!(classify_index(true, true), IndexAction::Keep);
        assert_eq!(classify_index(true, false), IndexAction::DropOrphan);
    }

    #[test]
    fn extract_index_name_handles_concurrently_and_quotes() {
        assert_eq!(
            extract_index_name(
                "CREATE INDEX CONCURRENTLY pi_t_x_abc123 ON \"public\".\"t\" USING btree (\"x\")"
            ),
            Some("pi_t_x_abc123".to_string())
        );
        assert_eq!(
            extract_index_name("CREATE INDEX \"pi_q\" ON t (a)"),
            Some("pi_q".to_string())
        );
        assert_eq!(extract_index_name("VACUUM t"), None);
    }
}
