//! Self-contained edtech demo (blueprint §6): schema, synthetic data, and a
//! representative workload with clear (but un-indexed) hot query patterns.

use sqlx::PgPool;

use crate::catalog::{self, WorkloadQuery};

const SCHEMA_SQL: &str = include_str!("../demo/schema.sql");

/// Create the demo schema (drops any previous demo tables).
pub async fn schema(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::raw_sql(SCHEMA_SQL).execute(pool).await?;
    println!("✓ demo schema created (public.*, primary keys only)");
    Ok(())
}

/// Populate the schema with synthetic data and refresh planner statistics.
pub async fn seed(pool: &PgPool) -> anyhow::Result<()> {
    let seed_sql = r#"
    INSERT INTO public.schools
      SELECT g, 'School '||g, (ARRAY['north','south','east','west'])[1+(g%4)]
      FROM generate_series(1,50) g;

    INSERT INTO public.students
      SELECT g, 1+(random()*49)::int, 'Student '||g, 1+(random()*11)::int,
             now() - (random()*400||' days')::interval
      FROM generate_series(1,5000) g;

    INSERT INTO public.classes
      SELECT g, 1+(random()*49)::int,
             (ARRAY['math','science','history','language','art'])[1+(g%5)],
             1+(random()*300)::int
      FROM generate_series(1,500) g;

    INSERT INTO public.enrollments
      SELECT g, 1+(random()*4999)::int, 1+(random()*499)::int,
             now() - (random()*300||' days')::interval,
             (ARRAY['active','active','active','dropped','completed'])[1+(g%5)]
      FROM generate_series(1,30000) g;

    INSERT INTO public.assignments
      SELECT g, 1+(random()*499)::int, 'Assignment '||g,
             now() + (random()*30||' days')::interval
      FROM generate_series(1,5000) g;

    INSERT INTO public.submissions
      SELECT g, 1+(random()*4999)::int, 1+(random()*4999)::int,
             now() - (random()*200||' days')::interval,
             (random() < 0.6), (random()*100)::int
      FROM generate_series(1,100000) g;

    INSERT INTO public.student_progress
      SELECT g, 1+(random()*4999)::int, 1+(random()*499)::int, 1+(random()*49)::int,
             (ARRAY['in_progress','completed','not_started'])[1+(g%3)],
             (random()*100)::int,
             now() - (random()*365||' days')::interval
      FROM generate_series(1,150000) g;

    INSERT INTO public.activity_events
      SELECT g, 1+(random()*4999)::int,
             (ARRAY['login','view','submit','comment'])[1+(g%4)],
             now() - (random()*365||' days')::interval
      FROM generate_series(1,200000) g;

    ANALYZE;
    "#;
    sqlx::raw_sql(seed_sql).execute(pool).await?;
    println!("✓ demo data seeded (~490k rows) and ANALYZEd");
    Ok(())
}

/// The representative workload — concrete, EXPLAIN-able queries with weights.
pub fn workload_queries() -> Vec<WorkloadQuery> {
    let q = |fp: &str, w: f64, sql: &str| WorkloadQuery {
        fingerprint: fp.to_string(),
        query_text: sql.trim().to_string(),
        weight: w,
        label: Some(fp.to_string()),
    };
    vec![
        q("student_dashboard", 40.0,
          "SELECT id, class_id, status, score, created_at FROM student_progress WHERE student_id = 1234 ORDER BY created_at DESC LIMIT 20"),
        q("class_progress", 25.0,
          "SELECT student_id, score FROM student_progress WHERE class_id = 42 AND status = 'completed'"),
        q("recent_activity", 30.0,
          "SELECT event_type, occurred_at FROM activity_events WHERE student_id = 999 ORDER BY occurred_at DESC LIMIT 50"),
        q("ungraded_submissions", 15.0,
          "SELECT id, student_id, submitted_at FROM submissions WHERE assignment_id = 77 AND graded = false"),
        q("student_enrollments", 20.0,
          "SELECT class_id, status FROM enrollments WHERE student_id = 555"),
        q("school_leaderboard", 10.0,
          "SELECT student_id, sum(score) AS total FROM student_progress WHERE school_id = 7 GROUP BY student_id ORDER BY total DESC LIMIT 10"),
    ]
}

/// Register the workload and run each query repeatedly to build up hot patterns
/// in `pg_stat_statements`.
pub async fn load(pool: &PgPool, iterations: u32) -> anyhow::Result<()> {
    let queries = workload_queries();
    for q in &queries {
        catalog::upsert_workload(pool, q).await?;
    }
    for _ in 0..iterations {
        for q in &queries {
            // Weight the number of executions loosely by frequency.
            let reps = ((q.weight / 10.0).ceil() as u32).max(1);
            for _ in 0..reps {
                let _ = sqlx::query(&q.query_text).execute(pool).await?;
            }
        }
    }
    println!(
        "✓ workload loaded: {} representative queries registered and exercised",
        queries.len()
    );
    Ok(())
}
