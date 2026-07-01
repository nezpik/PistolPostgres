-- Automatic workload capture: mark whether a workload entry is a parameterized
-- (normalized) query captured from pg_stat_statements. Parameterized queries
-- are evaluated with EXPLAIN (GENERIC_PLAN); only concrete queries can be timed
-- with EXPLAIN (ANALYZE).
ALTER TABLE pistol.workload
    ADD COLUMN IF NOT EXISTS parameterized BOOLEAN NOT NULL DEFAULT false;
