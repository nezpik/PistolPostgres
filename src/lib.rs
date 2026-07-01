//! PistolPostgres — controlled evolutionary self-optimization on top of
//! Postgres. Library crate exposing the engine components; the `pistol` binary
//! is a thin wrapper over `cli::dispatch`. See README.md / docs/DESIGN.md.

pub mod apply;
pub mod catalog;
pub mod cli;
pub mod config;
pub mod db;
pub mod demo;
pub mod engine;
pub mod evaluator;
pub mod genome;
pub mod policy;
pub mod proposer;
pub mod telemetry;
