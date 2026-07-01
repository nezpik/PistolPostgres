//! Proposal engine (blueprint §4.2). A `Proposer` turns telemetry-derived
//! candidates into ranked, evaluated `Proposal`s. The evolutionary proposer is
//! always available and fully offline; an optional Claude-backed proposer can
//! be compiled in behind the `llm` feature.

pub mod evolutionary;
#[cfg(feature = "llm")]
pub mod llm;

use serde::{Deserialize, Serialize};

use crate::catalog::{TableStat, WorkloadQuery};
use crate::config::Config;
use crate::evaluator::{Evaluation, Evaluator};
use crate::genome::{Genome, IndexSpec};
use crate::telemetry::IndexCandidate;
use std::collections::HashMap;

/// A single candidate change (an index to add), ranked and evaluated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proposal {
    pub id: String,
    pub change_type: String,
    pub target_object: String,
    pub index: IndexSpec,
    pub ddl: String,
    pub rationale: String,
    pub fitness: f64,
    pub source: String,
    pub genome_variants_tested: usize,
    pub evaluation: Option<Evaluation>,
}

/// Context passed to a proposer for one evolution cycle.
pub struct ProposalContext<'a> {
    pub candidates: Vec<IndexCandidate>,
    #[allow(dead_code)] // used by the optional LLM proposer (feature = "llm")
    pub workload: &'a [WorkloadQuery],
    pub table_stats: &'a HashMap<String, TableStat>,
    pub genome: &'a Genome,
    pub config: &'a Config,
}

/// Dispatch over the available proposer implementations.
pub enum Proposer {
    Evolutionary(evolutionary::EvolutionaryProposer),
    #[cfg(feature = "llm")]
    Llm(llm::LlmProposer),
}

impl Proposer {
    /// Select the proposer. The evolutionary core is the default; when built
    /// with `--features llm` and `PISTOL_PROPOSER=llm`, use the Claude-backed
    /// proposer (which still routes every candidate through the same evaluator
    /// and policy gates).
    pub fn from_env() -> Self {
        #[cfg(feature = "llm")]
        {
            if std::env::var("PISTOL_PROPOSER")
                .map(|v| v == "llm")
                .unwrap_or(false)
            {
                return Proposer::Llm(llm::LlmProposer::default());
            }
        }
        Proposer::Evolutionary(evolutionary::EvolutionaryProposer)
    }

    pub async fn propose(
        &self,
        evaluator: &Evaluator<'_>,
        ctx: &ProposalContext<'_>,
    ) -> anyhow::Result<Vec<Proposal>> {
        match self {
            Proposer::Evolutionary(p) => p.propose(evaluator, ctx).await,
            #[cfg(feature = "llm")]
            Proposer::Llm(p) => p.propose(evaluator, ctx).await,
        }
    }
}
