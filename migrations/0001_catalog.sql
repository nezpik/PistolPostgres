-- PistolPostgres evolution catalog (blueprint §4.5, extended for the loop).
-- All evolution state is first-class, queryable Postgres data.

CREATE SCHEMA IF NOT EXISTS pistol;

-- Extensions the engine relies on. hypopg powers zero-cost evaluation;
-- pg_stat_statements powers workload telemetry.
CREATE EXTENSION IF NOT EXISTS hypopg;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ---------------------------------------------------------------------------
-- Proposals queue / history (blueprint §4.5)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.proposals (
    id                  TEXT PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    status              TEXT NOT NULL DEFAULT 'proposed'
        CHECK (status IN ('proposed','evaluating','approved','applied','rejected','rolled_back')),
    change_type         TEXT NOT NULL DEFAULT 'index',
    target_object       TEXT,
    proposal_json       JSONB NOT NULL,
    evaluation_results  JSONB,
    policy_decision     JSONB,
    applied_history_id  BIGINT
);

-- ---------------------------------------------------------------------------
-- Immutable evolution audit log (blueprint §4.5)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.evolution_history (
    id              BIGSERIAL PRIMARY KEY,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    proposal_id     TEXT,
    change_type     TEXT,                 -- 'index','partition','materialized_view','schema_extension'
    target_object   TEXT,
    ddl_executed    TEXT,
    rationale       TEXT,                 -- AI + metric explanation
    before_metrics  JSONB,
    after_metrics   JSONB,
    actual_impact   JSONB,
    rollback_ddl    TEXT,
    triggered_by    TEXT,                 -- 'auto','hermes','manual'
    genome_context  JSONB,
    status          TEXT NOT NULL DEFAULT 'applied'
        CHECK (status IN ('applied','rolled_back')),
    CONSTRAINT evolution_history_proposal_id_key UNIQUE (proposal_id)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'proposals_applied_history_fk'
    ) THEN
        ALTER TABLE pistol.proposals
            ADD CONSTRAINT proposals_applied_history_fk
            FOREIGN KEY (applied_history_id) REFERENCES pistol.evolution_history(id)
            ON DELETE SET NULL;
    END IF;
END $$;

-- Guard the audit log against silent tampering: rows may only be inserted or
-- have their status flipped applied -> rolled_back (with impact recorded).
CREATE OR REPLACE FUNCTION pistol.evolution_history_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.id <> NEW.id OR OLD.ddl_executed IS DISTINCT FROM NEW.ddl_executed
       OR OLD.applied_at <> NEW.applied_at OR OLD.rollback_ddl IS DISTINCT FROM NEW.rollback_ddl THEN
        RAISE EXCEPTION 'pistol.evolution_history is append-only (immutable core columns)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS evolution_history_guard ON pistol.evolution_history;
CREATE TRIGGER evolution_history_guard
    BEFORE UPDATE ON pistol.evolution_history
    FOR EACH ROW EXECUTE FUNCTION pistol.evolution_history_guard();

-- ---------------------------------------------------------------------------
-- Current active "genome" snapshot (blueprint §4.5)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.current_genome (
    id                BIGSERIAL PRIMARY KEY,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    active_indexes    JSONB NOT NULL DEFAULT '[]'::jsonb,
    active_partitions JSONB NOT NULL DEFAULT '[]'::jsonb,
    active_mvs        JSONB NOT NULL DEFAULT '[]'::jsonb,
    fitness_snapshot  JSONB
);

-- ---------------------------------------------------------------------------
-- Telemetry snapshots (blueprint §4.1)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.telemetry_snapshots (
    id            BIGSERIAL PRIMARY KEY,
    captured_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    table_stats   JSONB NOT NULL,   -- per-table: rows, seq/idx scans, writes, size
    index_stats   JSONB NOT NULL,   -- per-index: scans, size
    query_stats   JSONB NOT NULL    -- pg_stat_statements digest
);

-- ---------------------------------------------------------------------------
-- Workload catalog: concrete representative queries.
-- hypopg must EXPLAIN concrete SQL, and pg_stat_statements only keeps
-- normalized text, so we keep a runnable representative per fingerprint.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.workload (
    id            BIGSERIAL PRIMARY KEY,
    fingerprint   TEXT UNIQUE NOT NULL,   -- stable id for the query shape
    query_text    TEXT NOT NULL,          -- concrete, EXPLAIN-able SQL
    weight        DOUBLE PRECISION NOT NULL DEFAULT 1.0,  -- call frequency
    label         TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Declarative policies (blueprint §4.4) — overrides for config defaults.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pistol.policies (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
