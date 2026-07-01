//! Evolutionary / stochastic proposer (blueprint §4.2, §7).
//!
//! A small genetic search over index designs: it seeds a population from
//! workload-derived candidate columns, then mutates and crosses them over,
//! scoring each with the hypopg evaluator against a multi-objective fitness
//! function. The RNG is seedable — fixed seed ⇒ reproducible proposals (tests
//! & demos), entropy seed ⇒ genuine exploration in production. This is the
//! "non-deterministic yet reliable" core.

use std::collections::HashMap;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::{Proposal, ProposalContext};
use crate::config::Fitness;
use crate::evaluator::{Evaluation, Evaluator};
use crate::genome::{Genome, IndexColumn, IndexSpec};

pub struct EvolutionaryProposer;

/// Per-table pool of indexable columns discovered from the workload.
struct TablePool {
    schema: String,
    table: String,
    eq: Vec<String>,
    sort: Vec<(String, bool)>,
    writes: i64,
}

impl EvolutionaryProposer {
    pub async fn propose(
        &self,
        evaluator: &Evaluator<'_>,
        ctx: &ProposalContext<'_>,
    ) -> anyhow::Result<Vec<Proposal>> {
        let evo = &ctx.config.evolution;
        let fit = &ctx.config.fitness;

        // Build per-table column pools, filtered by workload support.
        let total_support: f64 = ctx
            .candidates
            .iter()
            .map(|c| c.support)
            .sum::<f64>()
            .max(1.0);
        let pools = build_pools(ctx, total_support, evo.max_columns_per_index);
        if pools.is_empty() {
            return Ok(vec![]);
        }
        let max_writes = pools.iter().map(|p| p.writes).max().unwrap_or(1).max(1);
        let pool_by_table: HashMap<(String, String), &TablePool> = pools
            .iter()
            .map(|p| ((p.schema.clone(), p.table.clone()), p))
            .collect();

        // Seed the gene pool with plausible index specs.
        let universe = seed_specs(&pools, evo.max_columns_per_index);
        if universe.is_empty() {
            return Ok(vec![]);
        }

        let mut rng = if evo.seed == 0 {
            ChaCha8Rng::from_entropy()
        } else {
            ChaCha8Rng::seed_from_u64(evo.seed)
        };

        // Fitness cache keyed by index signature (also caps evaluation cost).
        let mut cache: HashMap<String, (Evaluation, f64)> = HashMap::new();

        // Initial population sampled from the universe.
        let mut population: Vec<IndexSpec> = Vec::new();
        for _ in 0..evo.population_size {
            if let Some(spec) = universe.choose(&mut rng) {
                population.push(spec.clone());
            }
        }

        for spec in &population {
            evaluate_cached(
                evaluator, spec, ctx.genome, fit, max_writes, ctx, &mut cache,
            )
            .await?;
        }

        for _ in 0..evo.generations {
            let mut offspring: Vec<IndexSpec> = Vec::new();
            for _ in 0..evo.population_size {
                let parent_a = tournament(&population, &cache, &mut rng);
                let parent_b = tournament(&population, &cache, &mut rng);
                let mut child = crossover(
                    &parent_a,
                    &parent_b,
                    &pool_by_table,
                    evo.max_columns_per_index,
                );
                if rng.gen::<f64>() < evo.mutation_rate {
                    child = mutate(&child, &pool_by_table, evo.max_columns_per_index, &mut rng);
                }
                offspring.push(child);
            }
            // Occasional random immigrant keeps diversity up.
            if let Some(imm) = universe.choose(&mut rng) {
                offspring.push(imm.clone());
            }

            for spec in &offspring {
                evaluate_cached(
                    evaluator, spec, ctx.genome, fit, max_writes, ctx, &mut cache,
                )
                .await?;
            }

            // Elitist survival: keep the best `population_size` seen among
            // current population ∪ offspring.
            let mut combined = population.clone();
            combined.extend(offspring);
            combined.sort_by(|a, b| {
                fitness_of(b, &cache)
                    .partial_cmp(&fitness_of(a, &cache))
                    .unwrap()
                    .then_with(|| a.signature().cmp(&b.signature()))
            });
            dedup_specs(&mut combined);
            combined.truncate(evo.population_size);
            population = combined;
        }

        // Rank every distinct spec we evaluated; emit the top few as proposals.
        let mut ranked: Vec<(IndexSpec, Evaluation, f64)> = universe
            .iter()
            .chain(population.iter())
            .map(|s| s.signature())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .filter_map(|sig| {
                cache.get(&sig).map(|(e, f)| {
                    // recover the spec from population/universe by signature
                    let spec = population
                        .iter()
                        .chain(universe.iter())
                        .find(|s| s.signature() == sig)
                        .cloned()
                        .unwrap();
                    (spec, e.clone(), *f)
                })
            })
            .collect();
        // Sort by fitness desc, breaking ties on signature so the ranking (and
        // thus the chosen proposal) is fully deterministic for a given seed —
        // independent of HashSet iteration order.
        ranked.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap()
                .then_with(|| a.0.signature().cmp(&b.0.signature()))
        });

        let variants = cache.len();
        let proposals = ranked
            .into_iter()
            .filter(|(_, _, f)| *f > 0.0)
            .take(5)
            .map(|(spec, eval, fitness)| build_proposal(spec, eval, fitness, variants))
            .collect();
        Ok(proposals)
    }
}

fn build_pools(ctx: &ProposalContext<'_>, total_support: f64, max_cols: usize) -> Vec<TablePool> {
    let min_support = ctx.config.evolution.min_column_support * total_support;
    // Aggregate by table so every eligible candidate for a table contributes its
    // columns (guards against duplicate candidates for the same table).
    let mut by_table: HashMap<(String, String), TablePool> = HashMap::new();
    for c in ctx
        .candidates
        .iter()
        .filter(|c| {
            c.support >= min_support && (!c.eq_columns.is_empty() || !c.sort_columns.is_empty())
        })
        .filter(|c| !is_protected(ctx, &c.schema, &c.table))
    {
        let key = format!("{}.{}", c.schema, c.table);
        let writes = ctx.table_stats.get(&key).map(|s| s.writes).unwrap_or(0);
        let pool = by_table
            .entry((c.schema.clone(), c.table.clone()))
            .or_insert_with(|| TablePool {
                schema: c.schema.clone(),
                table: c.table.clone(),
                eq: Vec::new(),
                sort: Vec::new(),
                writes,
            });
        for col in c.eq_columns.iter() {
            if pool.eq.len() < max_cols && !pool.eq.contains(col) {
                pool.eq.push(col.clone());
            }
        }
        for col in c.sort_columns.iter() {
            if pool.sort.len() < max_cols && !pool.sort.contains(col) {
                pool.sort.push(col.clone());
            }
        }
    }
    let mut pools: Vec<TablePool> = by_table
        .into_values()
        .filter(|p| !p.eq.is_empty() || !p.sort.is_empty())
        .collect();
    // HashMap iteration order is nondeterministic; sort so the seeded search is
    // fully reproducible for a given seed.
    pools.sort_by(|a, b| (&a.schema, &a.table).cmp(&(&b.schema, &b.table)));
    pools
}

fn is_protected(ctx: &ProposalContext<'_>, schema: &str, table: &str) -> bool {
    let pol = &ctx.config.policy;
    pol.protected_schemas.iter().any(|s| s == schema)
        || pol
            .protected_tables
            .iter()
            .any(|t| t == table || t == &format!("{schema}.{table}"))
}

/// Generate an initial universe of plausible index specs from the pools.
fn seed_specs(pools: &[TablePool], max_cols: usize) -> Vec<IndexSpec> {
    let mut specs: Vec<IndexSpec> = Vec::new();
    for p in pools {
        // Single equality columns.
        for c in &p.eq {
            specs.push(spec(p, vec![IndexColumn::asc(c)]));
        }
        // Leading eq col + a second eq col.
        if let Some(lead) = p.eq.first() {
            for c in p.eq.iter().skip(1) {
                specs.push(spec(p, vec![IndexColumn::asc(lead), IndexColumn::asc(c)]));
            }
            // Leading eq col + trailing sort col (great for ORDER BY ... LIMIT).
            for (s, desc) in &p.sort {
                if s != lead {
                    let col = if *desc {
                        IndexColumn::desc(s)
                    } else {
                        IndexColumn::asc(s)
                    };
                    specs.push(spec(p, vec![IndexColumn::asc(lead), col]));
                }
            }
        }
        // A lone leading sort column (helps top-N scans).
        if p.eq.is_empty() {
            if let Some((s, desc)) = p.sort.first() {
                let col = if *desc {
                    IndexColumn::desc(s)
                } else {
                    IndexColumn::asc(s)
                };
                specs.push(spec(p, vec![col]));
            }
        }
    }
    for s in specs.iter_mut() {
        s.columns.truncate(max_cols);
    }
    dedup_specs(&mut specs);
    specs
}

fn spec(p: &TablePool, columns: Vec<IndexColumn>) -> IndexSpec {
    IndexSpec {
        schema: p.schema.clone(),
        table: p.table.clone(),
        columns,
        method: "btree".into(),
    }
}

fn dedup_specs(specs: &mut Vec<IndexSpec>) {
    let mut seen = std::collections::HashSet::new();
    specs.retain(|s| !s.columns.is_empty() && seen.insert(s.signature()));
}

fn tournament(
    population: &[IndexSpec],
    cache: &HashMap<String, (Evaluation, f64)>,
    rng: &mut ChaCha8Rng,
) -> IndexSpec {
    let a = population.choose(rng).cloned().unwrap();
    let b = population.choose(rng).cloned().unwrap();
    if fitness_of(&a, cache) >= fitness_of(&b, cache) {
        a
    } else {
        b
    }
}

fn crossover(
    a: &IndexSpec,
    b: &IndexSpec,
    pools: &HashMap<(String, String), &TablePool>,
    max_cols: usize,
) -> IndexSpec {
    if a.schema != b.schema || a.table != b.table {
        return a.clone();
    }
    let _ = pools;
    let cut = (a.columns.len() / 2).max(1);
    let mut cols: Vec<IndexColumn> = a.columns[..cut.min(a.columns.len())].to_vec();
    for c in &b.columns {
        if !cols.iter().any(|x| x.name == c.name) {
            cols.push(c.clone());
        }
    }
    cols.truncate(max_cols);
    IndexSpec {
        schema: a.schema.clone(),
        table: a.table.clone(),
        columns: cols,
        method: "btree".into(),
    }
}

fn mutate(
    spec: &IndexSpec,
    pools: &HashMap<(String, String), &TablePool>,
    max_cols: usize,
    rng: &mut ChaCha8Rng,
) -> IndexSpec {
    let mut out = spec.clone();
    let pool = match pools.get(&(spec.schema.clone(), spec.table.clone())) {
        Some(p) => *p,
        None => return out,
    };
    let all_cols: Vec<(String, bool)> = pool
        .eq
        .iter()
        .map(|c| (c.clone(), false))
        .chain(pool.sort.iter().cloned())
        .collect();

    match rng.gen_range(0..4) {
        0 => {
            // Add a column not already present.
            let missing: Vec<&(String, bool)> = all_cols
                .iter()
                .filter(|(c, _)| !out.columns.iter().any(|x| &x.name == c))
                .collect();
            if let Some((c, desc)) = missing.choose(rng) {
                if out.columns.len() < max_cols {
                    out.columns.push(IndexColumn {
                        name: c.clone(),
                        desc: *desc,
                    });
                }
            }
        }
        1 => {
            // Drop the trailing column.
            if out.columns.len() > 1 {
                out.columns.pop();
            }
        }
        2 => {
            // Swap two positions.
            if out.columns.len() >= 2 {
                let i = rng.gen_range(0..out.columns.len());
                let j = rng.gen_range(0..out.columns.len());
                out.columns.swap(i, j);
            }
        }
        _ => {
            // Toggle sort direction on a random column.
            if !out.columns.is_empty() {
                let i = rng.gen_range(0..out.columns.len());
                out.columns[i].desc = !out.columns[i].desc;
            }
        }
    }
    if out.columns.is_empty() {
        return spec.clone();
    }
    out
}

fn fitness_of(spec: &IndexSpec, cache: &HashMap<String, (Evaluation, f64)>) -> f64 {
    cache
        .get(&spec.signature())
        .map(|(_, f)| *f)
        .unwrap_or(f64::MIN)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_cached(
    evaluator: &Evaluator<'_>,
    spec: &IndexSpec,
    genome: &Genome,
    fit: &Fitness,
    max_writes: i64,
    ctx: &ProposalContext<'_>,
    cache: &mut HashMap<String, (Evaluation, f64)>,
) -> anyhow::Result<()> {
    let sig = spec.signature();
    if cache.contains_key(&sig) {
        return Ok(());
    }
    // Tolerate an unevaluable candidate (e.g. an invalid column combination):
    // skip it rather than aborting the whole cycle. Reliability over coverage.
    let eval = match evaluator.evaluate(spec).await {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(index = %sig, error = %e, "skipping unevaluable candidate");
            return Ok(());
        }
    };
    let key = format!("{}.{}", spec.schema, spec.table);
    let writes = ctx.table_stats.get(&key).map(|s| s.writes).unwrap_or(0);
    let f = score(&eval, spec, genome, fit, writes, max_writes);
    cache.insert(sig, (eval, f));
    Ok(())
}

/// Multi-objective fitness (blueprint §4.2). Positive is good.
fn score(
    eval: &Evaluation,
    spec: &IndexSpec,
    genome: &Genome,
    fit: &Fitness,
    writes: i64,
    max_writes: i64,
) -> f64 {
    let benefit = fit.w_cost * (eval.predicted_improvement_pct / 100.0);

    let storage_mb = eval.storage_bytes as f64 / (1024.0 * 1024.0);
    let storage_penalty = fit.w_storage * (storage_mb / 100.0);

    let write_norm = writes as f64 / max_writes as f64; // 0..1
                                                        // More columns cost more to maintain on writes.
    let write_penalty = fit.w_write_amp * write_norm * (spec.columns.len() as f64 / 3.0);

    let redundancy = if genome.indexes.iter().any(|i| i.overlaps(spec)) {
        1.0
    } else {
        0.0
    };
    let redundancy_penalty = fit.w_redundancy * redundancy;

    // Soft penalty for any query regression (hard gate lives in policy).
    let regression_penalty = if eval.worst_regression_pct > 0.0 {
        eval.worst_regression_pct / 100.0
    } else {
        0.0
    };

    benefit - storage_penalty - write_penalty - redundancy_penalty - regression_penalty
}

fn build_proposal(spec: IndexSpec, eval: Evaluation, fitness: f64, variants: usize) -> Proposal {
    let storage_mb = eval.storage_bytes as f64 / (1024.0 * 1024.0);
    let rationale = format!(
        "Workload-driven index on {}.{} ({}). Predicted {:.1}% weighted plan-cost reduction across {} queries; ~{:.1} MB; worst regression {:.1}%. Columns chosen from equality/sort predicates; survived {} evaluated variants.",
        spec.schema,
        spec.table,
        spec.columns.iter().map(|c| if c.desc { format!("{} DESC", c.name) } else { c.name.clone() }).collect::<Vec<_>>().join(", "),
        eval.predicted_improvement_pct,
        eval.per_query.len(),
        storage_mb,
        eval.worst_regression_pct,
        variants,
    );
    Proposal {
        id: String::new(), // assigned by the engine
        change_type: "index".into(),
        target_object: format!("{}.{}", spec.schema, spec.table),
        ddl: spec.create_ddl(true),
        index: spec,
        rationale,
        fitness,
        source: "evolutionary".into(),
        genome_variants_tested: variants,
        evaluation: Some(eval),
    }
}
