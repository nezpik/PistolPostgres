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
        // A failed concurrent build can leave an INVALID index behind. Only drop
        // *that* — never a valid index a concurrent run may have created under
        // the same (deterministic) name.
        if index_is_invalid(pool, &index.schema, &index.index_name())
            .await
            .unwrap_or(false)
        {
            let _ = (&mut *conn).execute(index.drop_ddl(true).as_str()).await;
        }
        return Err(anyhow::anyhow!("index build failed: {e}"));
    }
    analyze(&mut conn, index).await;
    Ok(())
}

/// Drop an index online (the reversal of `build_index_online`).
pub async fn drop_index_online(pool: &PgPool, index: &IndexSpec) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    (&mut *conn).execute(index.drop_ddl(true).as_str()).await?;
    analyze(&mut conn, index).await;
    Ok(())
}

/// Drop an index online by schema-qualified name (used by reconciliation, which
/// works from catalog/physical names rather than a full `IndexSpec`).
pub async fn drop_index_by_name(pool: &PgPool, schema: &str, name: &str) -> anyhow::Result<()> {
    let ddl = format!("DROP INDEX CONCURRENTLY IF EXISTS \"{schema}\".\"{name}\"");
    let mut conn = pool.acquire().await?;
    (&mut *conn).execute(ddl.as_str()).await?;
    Ok(())
}

/// Refresh planner stats after DDL. Best-effort: a failure here doesn't
/// invalidate the DDL, but we surface it (stale stats can skew the next plan).
async fn analyze(conn: &mut sqlx::PgConnection, index: &IndexSpec) {
    let sql = format!("ANALYZE {}", index.qualified_table());
    if let Err(e) = (&mut *conn).execute(sql.as_str()).await {
        tracing::warn!(
            table = %index.qualified_table(),
            error = %e,
            "ANALYZE after DDL failed; planner stats may be stale"
        );
    }
}

/// True only if an index with this name exists AND is marked invalid.
async fn index_is_invalid(pool: &PgPool, schema: &str, name: &str) -> anyhow::Result<bool> {
    let invalid: Option<bool> = sqlx::query_scalar(
        "SELECT NOT i.indisvalid
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(invalid.unwrap_or(false))
}

/// The real on-disk size of a built index (bytes). Returns 0 only when the index
/// genuinely does not exist; other errors propagate.
pub async fn index_size_bytes(pool: &PgPool, index: &IndexSpec) -> anyhow::Result<i64> {
    match sqlx::query_scalar::<_, i64>("SELECT pg_relation_size($1::regclass)")
        .bind(format!("\"{}\".\"{}\"", index.schema, index.index_name()))
        .fetch_one(pool)
        .await
    {
        Ok(size) => Ok(size),
        // 42P01 = undefined_table: the regclass name does not resolve.
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42P01") => Ok(0),
        Err(e) => Err(e.into()),
    }
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
