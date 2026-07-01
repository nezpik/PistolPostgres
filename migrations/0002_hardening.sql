-- Hardening follow-ups (addresses review feedback on 0001).
-- Kept as a separate migration because 0001 is already applied on deployed
-- databases; editing it would break sqlx checksum validation.

-- 1. Make the audit log genuinely append-only.
--    The only mutation ever permitted is recording a rollback: flipping
--    status applied -> rolled_back and setting actual_impact. Every other
--    column is frozen, and inserts are unaffected (BEFORE UPDATE only).
CREATE OR REPLACE FUNCTION pistol.evolution_history_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.status = 'applied'
       AND NEW.status = 'rolled_back'
       AND NEW.id            =  OLD.id
       AND NEW.applied_at    =  OLD.applied_at
       AND NEW.proposal_id   IS NOT DISTINCT FROM OLD.proposal_id
       AND NEW.change_type   IS NOT DISTINCT FROM OLD.change_type
       AND NEW.target_object IS NOT DISTINCT FROM OLD.target_object
       AND NEW.ddl_executed  IS NOT DISTINCT FROM OLD.ddl_executed
       AND NEW.rationale     IS NOT DISTINCT FROM OLD.rationale
       AND NEW.before_metrics IS NOT DISTINCT FROM OLD.before_metrics
       AND NEW.after_metrics  IS NOT DISTINCT FROM OLD.after_metrics
       AND NEW.rollback_ddl  IS NOT DISTINCT FROM OLD.rollback_ddl
       AND NEW.triggered_by  IS NOT DISTINCT FROM OLD.triggered_by
       AND NEW.genome_context IS NOT DISTINCT FROM OLD.genome_context
    THEN
        RETURN NEW; -- only actual_impact is allowed to change here
    END IF;
    RAISE EXCEPTION
        'pistol.evolution_history is append-only; only the applied -> rolled_back transition (setting actual_impact) is permitted';
END;
$$ LANGUAGE plpgsql;

-- 2. Scope the FK existence check to pistol.proposals (constraint names are not
--    globally unique), so a same-named constraint elsewhere can't leave this
--    table without its foreign key.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conname = 'proposals_applied_history_fk'
           AND conrelid = 'pistol.proposals'::regclass
    ) THEN
        ALTER TABLE pistol.proposals
            ADD CONSTRAINT proposals_applied_history_fk
            FOREIGN KEY (applied_history_id) REFERENCES pistol.evolution_history(id)
            ON DELETE SET NULL;
    END IF;
END $$;
