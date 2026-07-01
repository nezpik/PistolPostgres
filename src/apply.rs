//! Online, reversible DDL primitives (blueprint §4.6). The engine composes
//! these into a measured trial: build → measure → keep or drop. Everything here
//! is online (`CONCURRENTLY`) and every build has a matching, pre-computed undo.

use sqlx::{Executor, PgPool};

use crate::genome::IndexSpec;

/// Build an index online. `CREATE INDEX CONCURRENTLY` cannot run inside a
/// transaction block, so it is sent via the simple query protocol (passing
/// `&str` to `execute` runs it unprepared / autocommit). Refreshes planner
/// stats afterwards so the new index is actually considered.
pub async fn build_index_online(pool: &PgPool, index: &IndexSpec) -> anyhow::Result<()> {
    let ddl = index.create_ddl(true);
    let mut conn = pool.acquire().await?;
    if let Err(e) = (&mut *conn).execute(ddl.as_str()).await {
        // A failed concurrent build can leave an INVALID index — clean it up.
        let _ = (&mut *conn).execute(index.drop_ddl(true).as_str()).await;
        return Err(anyhow::anyhow!("index build failed: {e}"));
    }
    let analyze = format!("ANALYZE {}", index.qualified_table());
    let _ = (&mut *conn).execute(analyze.as_str()).await;
    Ok(())
}

/// Drop an index online (the reversal of `build_index_online`).
pub async fn drop_index_online(pool: &PgPool, index: &IndexSpec) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    (&mut *conn).execute(index.drop_ddl(true).as_str()).await?;
    let analyze = format!("ANALYZE {}", index.qualified_table());
    let _ = (&mut *conn).execute(analyze.as_str()).await;
    Ok(())
}

/// The real on-disk size of a built index (bytes), or 0 if absent.
pub async fn index_size_bytes(pool: &PgPool, index: &IndexSpec) -> anyhow::Result<i64> {
    let size: i64 = sqlx::query_scalar("SELECT pg_relation_size($1::regclass)")
        .bind(format!("\"{}\".\"{}\"", index.schema, index.index_name()))
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    Ok(size)
}

/// Execute a stored rollback DDL on demand (used by `pistol rollback <id>`).
pub async fn execute_rollback(pool: &PgPool, rollback_ddl: &str) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    (&mut *conn).execute(rollback_ddl).await?;
    Ok(())
}

/// True if a physical index with this name already exists (idempotency guard).
pub async fn index_exists(pool: &PgPool, schema: &str, name: &str) -> anyhow::Result<bool> {
    let exists: Option<bool> =
        sqlx::query_scalar("SELECT true FROM pg_indexes WHERE schemaname = $1 AND indexname = $2")
            .bind(schema)
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(exists.unwrap_or(false))
}
