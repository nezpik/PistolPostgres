//! Telemetry Collector (blueprint §4.1) + workload-driven index-candidate
//! extraction. We snapshot `pg_stat_*`, and we parse the representative
//! workload with a real SQL parser to discover which (table, columns) are worth
//! indexing — so proposals are grounded in actual query shapes, not guesses.

use std::collections::HashMap;

use serde_json::{json, Value};
use sqlparser::ast::{
    Expr, GroupByExpr, Join, JoinConstraint, JoinOperator, Query, Select, SetExpr, Statement,
    TableFactor, TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlx::{PgPool, Row};

use crate::catalog::{self, TableStat, WorkloadQuery};

pub struct Telemetry {
    pub snapshot_id: i64,
    pub workload: Vec<WorkloadQuery>,
    pub table_stats: HashMap<String, TableStat>,
}

/// Collect a telemetry snapshot and persist it to `pistol.telemetry_snapshots`.
pub async fn collect(pool: &PgPool) -> anyhow::Result<Telemetry> {
    let (table_json, table_stats) = table_stats(pool).await?;
    let index_json = index_stats(pool).await?;
    let query_json = query_stats(pool).await?;

    let snapshot_id =
        catalog::insert_telemetry(pool, &table_json, &index_json, &query_json).await?;
    let workload = catalog::fetch_workload(pool).await?;

    Ok(Telemetry {
        snapshot_id,
        workload,
        table_stats,
    })
}

async fn table_stats(pool: &PgPool) -> anyhow::Result<(Value, HashMap<String, TableStat>)> {
    let rows = sqlx::query(
        "SELECT schemaname AS schema, relname AS table, n_live_tup,
                seq_scan, COALESCE(idx_scan,0) AS idx_scan,
                (n_tup_ins + n_tup_upd + n_tup_del) AS writes,
                pg_relation_size(relid) AS bytes
           FROM pg_stat_user_tables",
    )
    .fetch_all(pool)
    .await?;

    let mut map = HashMap::new();
    let mut arr = Vec::new();
    for r in rows {
        let schema: String = r.get("schema");
        let table: String = r.get("table");
        let live: i64 = r.get("n_live_tup");
        let writes: i64 = r.get("writes");
        map.insert(
            format!("{schema}.{table}"),
            TableStat {
                writes,
                live_rows: live,
            },
        );
        arr.push(json!({
            "schema": schema,
            "table": table,
            "n_live_tup": live,
            "seq_scan": r.get::<i64,_>("seq_scan"),
            "idx_scan": r.get::<i64,_>("idx_scan"),
            "writes": writes,
            "bytes": r.get::<i64,_>("bytes"),
        }));
    }
    Ok((Value::Array(arr), map))
}

async fn index_stats(pool: &PgPool) -> anyhow::Result<Value> {
    let rows = sqlx::query(
        "SELECT schemaname AS schema, relname AS table, indexrelname AS index,
                COALESCE(idx_scan,0) AS idx_scan, pg_relation_size(indexrelid) AS bytes
           FROM pg_stat_user_indexes",
    )
    .fetch_all(pool)
    .await?;
    let arr: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "schema": r.get::<String,_>("schema"),
                "table": r.get::<String,_>("table"),
                "index": r.get::<String,_>("index"),
                "idx_scan": r.get::<i64,_>("idx_scan"),
                "bytes": r.get::<i64,_>("bytes"),
            })
        })
        .collect();
    Ok(Value::Array(arr))
}

async fn query_stats(pool: &PgPool) -> anyhow::Result<Value> {
    // pg_stat_statements may be empty; tolerate absence gracefully.
    let rows = sqlx::query(
        "SELECT calls, total_exec_time, mean_exec_time, rows, query
           FROM pg_stat_statements
          ORDER BY total_exec_time DESC LIMIT 50",
    )
    .fetch_all(pool)
    .await;
    let rows = match rows {
        Ok(r) => r,
        // Only the "relation does not exist" case (extension not installed) is a
        // benign "no data"; every other error is a real failure to surface.
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42P01") => {
            tracing::warn!("pg_stat_statements not available; skipping query telemetry");
            return Ok(Value::Array(vec![]));
        }
        Err(e) => return Err(e.into()),
    };
    let arr: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "calls": r.get::<i64,_>("calls"),
                "total_exec_time": r.get::<f64,_>("total_exec_time"),
                "mean_exec_time": r.get::<f64,_>("mean_exec_time"),
                "rows": r.get::<i64,_>("rows"),
                "query": r.get::<String,_>("query"),
            })
        })
        .collect();
    Ok(Value::Array(arr))
}

// --------------------------------------------------------------------------
// Workload-driven candidate extraction
// --------------------------------------------------------------------------

/// A promising set of columns to index on a table, aggregated across the
/// workload. `eq_columns` are equality/`IN`/group-by predicates (great leading
/// columns), `range_columns` are inequalities, `sort_columns` are ORDER BY keys.
#[derive(Debug, Clone)]
pub struct IndexCandidate {
    pub schema: String,
    pub table: String,
    pub eq_columns: Vec<String>,
    pub range_columns: Vec<String>,
    pub sort_columns: Vec<(String, bool)>, // (column, desc)
    pub support: f64,                      // summed weight of contributing queries
}

#[derive(Default)]
struct Acc {
    eq: Vec<(String, f64)>,
    range: Vec<(String, f64)>,
    sort: Vec<(String, bool)>,
    support: f64,
}

/// Extract candidates from the workload, then prune any columns that don't
/// actually exist on their table (e.g. SELECT-list aliases that leaked in via
/// `ORDER BY <alias>`). This keeps every downstream proposal buildable.
pub async fn candidates_validated(
    pool: &PgPool,
    workload: &[WorkloadQuery],
) -> anyhow::Result<Vec<IndexCandidate>> {
    let mut candidates = candidates_from_workload(workload);
    let real = real_columns(pool).await?;
    for c in candidates.iter_mut() {
        let key = (c.schema.clone(), c.table.clone());
        let cols = real.get(&key);
        let keep = |name: &str| cols.map(|s| s.contains(name)).unwrap_or(false);
        c.eq_columns.retain(|x| keep(x));
        c.range_columns.retain(|x| keep(x));
        c.sort_columns.retain(|(x, _)| keep(x));
    }
    candidates.retain(|c| {
        !c.eq_columns.is_empty() || !c.sort_columns.is_empty() || !c.range_columns.is_empty()
    });
    Ok(candidates)
}

async fn real_columns(
    pool: &PgPool,
) -> anyhow::Result<HashMap<(String, String), std::collections::HashSet<String>>> {
    let rows = sqlx::query(
        "SELECT table_schema, table_name, column_name
           FROM information_schema.columns
          WHERE table_schema NOT IN ('pg_catalog','information_schema')",
    )
    .fetch_all(pool)
    .await?;
    let mut map: HashMap<(String, String), std::collections::HashSet<String>> = HashMap::new();
    for r in rows {
        let schema: String = r.get("table_schema");
        let table: String = r.get("table_name");
        let col: String = r.get("column_name");
        map.entry((schema, table)).or_default().insert(col);
    }
    Ok(map)
}

/// Parse every workload query and derive per-table index candidates. Queries
/// that fail to parse are skipped (reliability over completeness).
pub fn candidates_from_workload(workload: &[WorkloadQuery]) -> Vec<IndexCandidate> {
    let dialect = PostgreSqlDialect {};
    let mut acc: HashMap<(String, String), Acc> = HashMap::new();

    for wq in workload {
        let statements = match Parser::parse_sql(&dialect, &wq.query_text) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(fingerprint = %wq.fingerprint, error = %e, "skipping unparsable workload query");
                continue;
            }
        };
        for stmt in statements {
            if let Statement::Query(q) = stmt {
                collect_query(&q, wq.weight, &mut acc);
            }
        }
    }

    let mut out: Vec<IndexCandidate> = acc
        .into_iter()
        .map(|((schema, table), a)| IndexCandidate {
            schema,
            table,
            eq_columns: dedup_by_weight(a.eq),
            range_columns: dedup_by_weight(a.range),
            sort_columns: dedup_sort(a.sort),
            support: a.support,
        })
        .collect();
    // Sort by support desc, breaking ties on (schema, table) so equal-support
    // workloads produce a deterministic candidate order.
    out.sort_by(|x, y| {
        y.support
            .partial_cmp(&x.support)
            .unwrap()
            .then_with(|| (&x.schema, &x.table).cmp(&(&y.schema, &y.table)))
    });
    out
}

fn dedup_by_weight(mut pairs: Vec<(String, f64)>) -> Vec<String> {
    let mut totals: HashMap<String, f64> = HashMap::new();
    for (c, w) in pairs.drain(..) {
        *totals.entry(c).or_default() += w;
    }
    let mut v: Vec<(String, f64)> = totals.into_iter().collect();
    // Sort by weight desc, then name for deterministic ordering.
    v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
    v.into_iter().map(|(c, _)| c).collect()
}

fn dedup_sort(pairs: Vec<(String, bool)>) -> Vec<(String, bool)> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for p in pairs {
        if seen.insert(p.0.clone()) {
            out.push(p);
        }
    }
    out
}

fn collect_query(q: &Query, weight: f64, acc: &mut HashMap<(String, String), Acc>) {
    if let SetExpr::Select(select) = q.body.as_ref() {
        let aliases = build_alias_map(&select.from);
        let base = single_base_table(&select.from);

        // WHERE + JOIN ON predicates.
        if let Some(sel) = &select.selection {
            walk_predicate(sel, &aliases, &base, weight, acc);
        }
        for twj in &select.from {
            for j in &twj.joins {
                collect_join(j, &aliases, &base, weight, acc);
            }
        }
        // GROUP BY -> treat as equality-ish leading candidates.
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for e in exprs {
                if let Some((tbl, col)) = resolve_column(e, &aliases, &base) {
                    touch(acc, &tbl).eq.push((col, weight));
                }
            }
        }
        collect_select_support(select, weight, acc);
    }

    // ORDER BY lives on the Query.
    if let Some(order_by) = &q.order_by {
        if let SetExpr::Select(select) = q.body.as_ref() {
            let aliases = build_alias_map(&select.from);
            let base = single_base_table(&select.from);
            for ob in &order_by.exprs {
                if let Some((tbl, col)) = resolve_column(&ob.expr, &aliases, &base) {
                    let desc = ob.asc == Some(false);
                    touch(acc, &tbl).sort.push((col, desc));
                }
            }
        }
    }
}

/// Add this query's weight to `support` exactly once per distinct base table it
/// references (support = summed weight of contributing queries), and ensure each
/// table has an `acc` entry even with no extractable predicate.
fn collect_select_support(select: &Select, weight: f64, acc: &mut HashMap<(String, String), Acc>) {
    let mut seen = std::collections::HashSet::new();
    let mut add = |tbl: (String, String), acc: &mut HashMap<(String, String), Acc>| {
        let first_time = seen.insert(tbl.clone());
        let e = acc.entry(tbl).or_default();
        if first_time {
            e.support += weight;
        }
    };
    for twj in &select.from {
        for tbl in tables_in(&twj.relation) {
            add(tbl, acc);
        }
        for j in &twj.joins {
            for tbl in tables_in(&j.relation) {
                add(tbl, acc);
            }
        }
    }
}

fn collect_join(
    j: &Join,
    aliases: &HashMap<String, (String, String)>,
    base: &Option<(String, String)>,
    weight: f64,
    acc: &mut HashMap<(String, String), Acc>,
) {
    let on = match &j.join_operator {
        JoinOperator::Inner(JoinConstraint::On(e))
        | JoinOperator::LeftOuter(JoinConstraint::On(e))
        | JoinOperator::RightOuter(JoinConstraint::On(e))
        | JoinOperator::FullOuter(JoinConstraint::On(e)) => Some(e),
        _ => None,
    };
    if let Some(e) = on {
        walk_predicate(e, aliases, base, weight, acc);
    }
}

fn walk_predicate(
    expr: &Expr,
    aliases: &HashMap<String, (String, String)>,
    base: &Option<(String, String)>,
    weight: f64,
    acc: &mut HashMap<(String, String), Acc>,
) {
    use sqlparser::ast::BinaryOperator as Op;
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            Op::And | Op::Or => {
                walk_predicate(left, aliases, base, weight, acc);
                walk_predicate(right, aliases, base, weight, acc);
            }
            Op::Eq => {
                record_side(left, right, true, aliases, base, weight, acc);
            }
            Op::Gt | Op::Lt | Op::GtEq | Op::LtEq | Op::NotEq => {
                record_side(left, right, false, aliases, base, weight, acc);
            }
            _ => {}
        },
        Expr::Between { expr, .. } => {
            if let Some((tbl, col)) = resolve_column(expr, aliases, base) {
                touch(acc, &tbl).range.push((col, weight));
            }
        }
        Expr::InList { expr, .. } => {
            if let Some((tbl, col)) = resolve_column(expr, aliases, base) {
                touch(acc, &tbl).eq.push((col, weight));
            }
        }
        Expr::Nested(inner) => walk_predicate(inner, aliases, base, weight, acc),
        _ => {}
    }
}

/// Record whichever side of a comparison is a column (the other being a value).
fn record_side(
    left: &Expr,
    right: &Expr,
    is_eq: bool,
    aliases: &HashMap<String, (String, String)>,
    base: &Option<(String, String)>,
    weight: f64,
    acc: &mut HashMap<(String, String), Acc>,
) {
    for side in [left, right] {
        if let Some((tbl, col)) = resolve_column(side, aliases, base) {
            let entry = touch(acc, &tbl);
            if is_eq {
                entry.eq.push((col, weight));
            } else {
                entry.range.push((col, weight));
            }
        }
    }
}

/// Ensure a table's accumulator exists and return it (support is accounted for
/// separately, once per query, in `collect_select_support`).
fn touch<'a>(acc: &'a mut HashMap<(String, String), Acc>, tbl: &(String, String)) -> &'a mut Acc {
    acc.entry(tbl.clone()).or_default()
}

/// Resolve an expression to a ((schema, table), column) reference, if it is a
/// column ref that we can attribute to a base table.
fn resolve_column(
    expr: &Expr,
    aliases: &HashMap<String, (String, String)>,
    base: &Option<(String, String)>,
) -> Option<((String, String), String)> {
    match expr {
        Expr::Identifier(id) => base.clone().map(|t| (t, id.value.clone())),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let prefix = &parts[parts.len() - 2].value;
            let col = parts[parts.len() - 1].value.clone();
            aliases.get(prefix).map(|t| (t.clone(), col))
        }
        _ => None,
    }
}

fn build_alias_map(from: &[TableWithJoins]) -> HashMap<String, (String, String)> {
    let mut map = HashMap::new();
    let mut insert = |factor: &TableFactor| {
        if let TableFactor::Table { name, alias, .. } = factor {
            let (schema, table) = object_name_to_schema_table(name);
            // Reference by both table name and alias.
            map.insert(table.clone(), (schema.clone(), table.clone()));
            if let Some(a) = alias {
                map.insert(a.name.value.clone(), (schema.clone(), table.clone()));
            }
        }
    };
    for twj in from {
        insert(&twj.relation);
        for j in &twj.joins {
            insert(&j.relation);
        }
    }
    map
}

fn tables_in(factor: &TableFactor) -> Vec<(String, String)> {
    if let TableFactor::Table { name, .. } = factor {
        vec![object_name_to_schema_table(name)]
    } else {
        vec![]
    }
}

/// If the query touches exactly one base table, bare column refs resolve to it.
fn single_base_table(from: &[TableWithJoins]) -> Option<(String, String)> {
    let mut tables = Vec::new();
    for twj in from {
        tables.extend(tables_in(&twj.relation));
        for j in &twj.joins {
            tables.extend(tables_in(&j.relation));
        }
    }
    if tables.len() == 1 {
        tables.into_iter().next()
    } else {
        None
    }
}

fn object_name_to_schema_table(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let parts = &name.0;
    match parts.len() {
        0 => ("public".to_string(), String::new()),
        1 => ("public".to_string(), parts[0].value.clone()),
        _ => (
            parts[parts.len() - 2].value.clone(),
            parts[parts.len() - 1].value.clone(),
        ),
    }
}
