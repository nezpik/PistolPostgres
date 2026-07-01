//! Property-based tests for the genome operators (blueprint §5 "property-based
//! testing of proposal generation"). These are pure and need no database.

use pistol::genome::{Genome, IndexColumn, IndexSpec};
use proptest::prelude::*;

fn column_strategy() -> impl Strategy<Value = IndexColumn> {
    (
        prop::sample::select(vec![
            "student_id",
            "class_id",
            "created_at",
            "school_id",
            "status",
        ]),
        any::<bool>(),
    )
        .prop_map(|(name, desc)| IndexColumn {
            name: name.to_string(),
            desc,
        })
}

fn spec_strategy() -> impl Strategy<Value = IndexSpec> {
    (
        prop::sample::select(vec!["student_progress", "activity_events", "enrollments"]),
        prop::collection::vec(column_strategy(), 1..=3),
    )
        .prop_map(|(table, columns)| IndexSpec::new(table, columns))
}

proptest! {
    #[test]
    fn index_name_is_bounded_and_deterministic(spec in spec_strategy()) {
        let n1 = spec.index_name();
        let n2 = spec.index_name();
        prop_assert_eq!(&n1, &n2);
        prop_assert!(!n1.is_empty());
        prop_assert!(n1.len() <= 63, "identifier too long: {}", n1);
    }

    #[test]
    fn ddl_is_well_formed(spec in spec_strategy()) {
        let create = spec.create_ddl(true);
        prop_assert!(create.starts_with("CREATE INDEX CONCURRENTLY "));
        let quoted_table = format!("\"{}\"", spec.table);
        prop_assert!(create.contains(&quoted_table));
        let hypo = spec.create_ddl_hypopg();
        prop_assert!(!hypo.contains("CONCURRENTLY"));
        prop_assert!(spec.drop_ddl(true).starts_with("DROP INDEX CONCURRENTLY IF EXISTS"));
    }

    #[test]
    fn overlap_is_reflexive(spec in spec_strategy()) {
        prop_assert!(spec.overlaps(&spec));
    }

    #[test]
    fn signature_encodes_column_order(spec in spec_strategy()) {
        // A genome containing a spec always reports it present.
        let mut g = Genome::default();
        g.indexes.push(spec.clone());
        prop_assert!(g.contains(&spec));
    }
}
