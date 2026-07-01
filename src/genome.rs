//! The "genome" — PistolPostgres' representation of a physical design.
//!
//! For the prototype the genome is a set of B-tree index specs; the types are
//! deliberately shaped so partitions / materialized views can be added later as
//! sibling `ChangeType`s without reworking the proposer or evaluator.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

/// A single indexed column, with sort direction (matters for ORDER BY support).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexColumn {
    pub name: String,
    #[serde(default)]
    pub desc: bool,
}

impl IndexColumn {
    pub fn asc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            desc: false,
        }
    }
    pub fn desc(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            desc: true,
        }
    }
    fn sql(&self) -> String {
        if self.desc {
            format!("\"{}\" DESC", self.name)
        } else {
            format!("\"{}\"", self.name)
        }
    }
}

/// A candidate / active index. The unit the evolutionary search mutates over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    #[serde(default = "default_schema")]
    pub schema: String,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    #[serde(default = "default_method")]
    pub method: String,
}

fn default_schema() -> String {
    "public".to_string()
}
fn default_method() -> String {
    "btree".to_string()
}

impl IndexSpec {
    #[allow(dead_code)] // convenience constructor used by tests
    pub fn new(table: impl Into<String>, columns: Vec<IndexColumn>) -> Self {
        Self {
            schema: default_schema(),
            table: table.into(),
            columns,
            method: default_method(),
        }
    }

    pub fn qualified_table(&self) -> String {
        format!("\"{}\".\"{}\"", self.schema, self.table)
    }

    /// Canonical string used for deduplication, caching and redundancy checks.
    pub fn signature(&self) -> String {
        let cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                if c.desc {
                    format!("{} DESC", c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect();
        format!(
            "{}.{} USING {} ({})",
            self.schema,
            self.table,
            self.method,
            cols.join(", ")
        )
    }

    /// Deterministic, collision-resistant, <=63-byte index name.
    pub fn index_name(&self) -> String {
        let cols: String = self
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join("_");
        let mut hasher = DefaultHasher::new();
        self.signature().hash(&mut hasher);
        let short = format!("{:06x}", (hasher.finish() as u32) & 0x00ff_ffff);
        let raw = format!("pi_{}_{}", self.table, cols);
        let max_body = 63 - 1 - short.len();
        let body = if raw.len() > max_body {
            &raw[..max_body]
        } else {
            raw.as_str()
        };
        format!("{}_{}", body, short)
    }

    /// DDL to create the index. `concurrently` is false for hypopg (which
    /// rejects it) and true for the real online apply path.
    pub fn create_ddl(&self, concurrently: bool) -> String {
        let cols: Vec<String> = self.columns.iter().map(|c| c.sql()).collect();
        format!(
            "CREATE INDEX {}{} ON {} USING {} ({})",
            if concurrently { "CONCURRENTLY " } else { "" },
            self.index_name(),
            self.qualified_table(),
            self.method,
            cols.join(", ")
        )
    }

    /// DDL for hypopg, which (as of 1.4.0) cannot resolve a schema-qualified
    /// table name. The evaluator sets `search_path` to the target schema first,
    /// so the unqualified table name resolves correctly.
    pub fn create_ddl_hypopg(&self) -> String {
        let cols: Vec<String> = self.columns.iter().map(|c| c.sql()).collect();
        format!(
            "CREATE INDEX {} ON \"{}\" USING {} ({})",
            self.index_name(),
            self.table,
            self.method,
            cols.join(", ")
        )
    }

    /// Reversal DDL, stored before apply so every change is undoable.
    pub fn drop_ddl(&self, concurrently: bool) -> String {
        format!(
            "DROP INDEX {}IF EXISTS \"{}\".\"{}\"",
            if concurrently { "CONCURRENTLY " } else { "" },
            self.schema,
            self.index_name()
        )
    }

    /// True when `self`'s columns are a leading prefix of `other`'s (or vice
    /// versa) on the same table — i.e. one index makes the other largely
    /// redundant. Used to penalize redundant proposals.
    pub fn overlaps(&self, other: &IndexSpec) -> bool {
        if self.schema != other.schema || self.table != other.table {
            return false;
        }
        let a: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        let b: Vec<&str> = other.columns.iter().map(|c| c.name.as_str()).collect();
        let n = a.len().min(b.len());
        a[..n] == b[..n]
    }
}

/// A set of index specs — the active or candidate physical design.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Genome {
    pub indexes: Vec<IndexSpec>,
}

impl Genome {
    pub fn contains(&self, spec: &IndexSpec) -> bool {
        let sig = spec.signature();
        self.indexes.iter().any(|i| i.signature() == sig)
    }

    pub fn index_count_on(&self, schema: &str, table: &str) -> usize {
        self.indexes
            .iter()
            .filter(|i| i.schema == schema && i.table == table)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(cols: Vec<IndexColumn>) -> IndexSpec {
        IndexSpec::new("student_progress", cols)
    }

    #[test]
    fn index_name_is_deterministic_and_bounded() {
        let s = spec(vec![
            IndexColumn::asc("student_id"),
            IndexColumn::desc("created_at"),
        ]);
        assert_eq!(s.index_name(), s.index_name());
        assert!(s.index_name().len() <= 63);
        // Very long table names still yield a valid, bounded identifier.
        let long = IndexSpec::new("a".repeat(80), vec![IndexColumn::asc("x")]);
        assert!(long.index_name().len() <= 63);
    }

    #[test]
    fn column_order_and_direction_change_identity() {
        let a = spec(vec![IndexColumn::asc("a"), IndexColumn::asc("b")]);
        let b = spec(vec![IndexColumn::asc("b"), IndexColumn::asc("a")]);
        let c = spec(vec![IndexColumn::asc("a"), IndexColumn::desc("b")]);
        assert_ne!(a.signature(), b.signature());
        assert_ne!(a.signature(), c.signature());
        assert_ne!(a.index_name(), b.index_name());
    }

    #[test]
    fn ddl_forms_are_well_shaped() {
        let s = spec(vec![
            IndexColumn::asc("student_id"),
            IndexColumn::desc("created_at"),
        ]);
        let ddl = s.create_ddl(true);
        assert!(ddl.starts_with("CREATE INDEX CONCURRENTLY "));
        assert!(ddl.contains("\"public\".\"student_progress\""));
        assert!(ddl.contains("\"created_at\" DESC"));
        // hypopg form is unqualified and never CONCURRENTLY.
        let h = s.create_ddl_hypopg();
        assert!(!h.contains("CONCURRENTLY"));
        assert!(h.contains("ON \"student_progress\""));
        assert!(s
            .drop_ddl(true)
            .contains("DROP INDEX CONCURRENTLY IF EXISTS"));
    }

    #[test]
    fn overlap_detects_prefix_redundancy() {
        let a = spec(vec![IndexColumn::asc("student_id")]);
        let b = spec(vec![
            IndexColumn::asc("student_id"),
            IndexColumn::desc("created_at"),
        ]);
        let c = spec(vec![IndexColumn::asc("class_id")]);
        assert!(a.overlaps(&b)); // a is a leading prefix of b
        assert!(!a.overlaps(&c));
        // Different table never overlaps.
        let other = IndexSpec::new("activity_events", vec![IndexColumn::asc("student_id")]);
        assert!(!a.overlaps(&other));
    }

    #[test]
    fn genome_counts_and_contains() {
        let mut g = Genome::default();
        let a = spec(vec![IndexColumn::asc("student_id")]);
        g.indexes.push(a.clone());
        assert!(g.contains(&a));
        assert_eq!(g.index_count_on("public", "student_progress"), 1);
        assert_eq!(g.index_count_on("public", "activity_events"), 0);
    }
}
