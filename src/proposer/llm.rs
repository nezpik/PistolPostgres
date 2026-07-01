//! Optional Claude-backed proposer (blueprint §4.2 "LLM Component").
//!
//! Compiled only with `--features llm`. It asks Claude for candidate indexes
//! given the workload + schema context, then runs each through the SAME hypopg
//! evaluator and policy gates as the evolutionary proposer — the LLM only
//! *suggests*; it never bypasses the safety path. Falls back to an empty result
//! (engine then relies on the evolutionary proposer) when no API key is set.

use serde::Deserialize;

use super::{Proposal, ProposalContext};
use crate::evaluator::Evaluator;
use crate::genome::{IndexColumn, IndexSpec};

pub struct LlmProposer {
    pub model: String,
}

impl Default for LlmProposer {
    fn default() -> Self {
        Self {
            model: std::env::var("PISTOL_LLM_MODEL").unwrap_or_else(|_| "claude-sonnet-5".into()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LlmIndex {
    table: String,
    #[serde(default)]
    schema: Option<String>,
    columns: Vec<LlmColumn>,
    #[serde(default)]
    rationale: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LlmColumn {
    name: String,
    #[serde(default)]
    desc: bool,
}

impl LlmProposer {
    pub async fn propose(
        &self,
        evaluator: &Evaluator<'_>,
        ctx: &ProposalContext<'_>,
    ) -> anyhow::Result<Vec<Proposal>> {
        let api_key = match std::env::var("ANTHROPIC_API_KEY") {
            Ok(k) if !k.is_empty() => k,
            _ => {
                tracing::warn!("ANTHROPIC_API_KEY not set; LLM proposer returns no candidates");
                return Ok(vec![]);
            }
        };
        let base = std::env::var("ANTHROPIC_BASE_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".into());

        let candidates_json = serde_json::to_string_pretty(
            &ctx.candidates
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "schema": c.schema, "table": c.table,
                        "equality_columns": c.eq_columns,
                        "range_columns": c.range_columns,
                        "sort_columns": c.sort_columns,
                    })
                })
                .collect::<Vec<_>>(),
        )?;
        // Do NOT send raw workload SQL (it can contain literals / tenant data)
        // to an external service; the extracted predicate metadata in
        // `candidates_json` already carries the structural signal the model
        // needs. Only fingerprints + weights are shared.
        let workload_json = serde_json::to_string_pretty(
            &ctx.workload
                .iter()
                .map(|w| serde_json::json!({ "fingerprint": w.fingerprint, "weight": w.weight }))
                .collect::<Vec<_>>(),
        )?;

        let prompt = format!(
            "You are a Postgres physical-design advisor. Given workload-derived \
             column candidates and representative queries, propose up to 5 B-tree \
             indexes most likely to reduce plan cost. Respond with ONLY a JSON array; \
             each element: {{\"schema\":\"public\",\"table\":\"...\",\"columns\":[{{\"name\":\"...\",\"desc\":false}}],\"rationale\":\"...\"}}.\n\n\
             CANDIDATES:\n{candidates_json}\n\nWORKLOAD:\n{workload_json}"
        );

        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 1024,
            "messages": [{ "role": "user", "content": prompt }],
        });

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let resp = client
            .post(format!("{base}/v1/messages"))
            .header("x-api-key", api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("Claude API error {status}: {text}");
        }
        let v: serde_json::Value = resp.json().await?;
        let text = v["content"][0]["text"].as_str().unwrap_or("").to_string();

        let json_slice = extract_json_array(&text).unwrap_or_else(|| "[]".into());
        let indexes: Vec<LlmIndex> = serde_json::from_str(&json_slice).unwrap_or_default();

        let mut proposals = Vec::new();
        for li in indexes {
            let spec = IndexSpec {
                schema: li.schema.unwrap_or_else(|| "public".into()),
                table: li.table,
                columns: li
                    .columns
                    .into_iter()
                    .map(|c| IndexColumn {
                        name: c.name,
                        desc: c.desc,
                    })
                    .collect(),
                method: "btree".into(),
            };
            if spec.columns.is_empty() {
                continue;
            }
            // Evaluate through the same safety harness.
            let eval = match evaluator.evaluate(&spec).await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(index = %spec.signature(), error = %e, "skipping unevaluable LLM index");
                    continue;
                }
            };
            let storage_mb = eval.storage_bytes as f64 / (1024.0 * 1024.0);
            let fitness = ctx.config.fitness.w_cost * (eval.predicted_improvement_pct / 100.0)
                - ctx.config.fitness.w_storage * (storage_mb / 100.0);
            let rationale = li
                .rationale
                .unwrap_or_else(|| "Claude-proposed index".into());
            proposals.push(Proposal {
                id: String::new(),
                change_type: "index".into(),
                target_object: format!("{}.{}", spec.schema, spec.table),
                ddl: spec.create_ddl(true),
                index: spec,
                rationale: format!("[llm] {rationale}"),
                fitness,
                source: "llm".into(),
                genome_variants_tested: 0,
                evaluation: Some(eval),
            });
        }
        proposals.sort_by(|a, b| b.fitness.partial_cmp(&a.fitness).unwrap());
        Ok(proposals)
    }
}

fn extract_json_array(text: &str) -> Option<String> {
    let start = text.find('[')?;
    let end = text.rfind(']')?;
    if end > start {
        Some(text[start..=end].to_string())
    } else {
        None
    }
}
