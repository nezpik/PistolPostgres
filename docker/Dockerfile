# Postgres with the two extensions PistolPostgres relies on:
#   - pg_stat_statements : workload telemetry (must be in shared_preload_libraries)
#   - hypopg             : zero-cost hypothetical-index evaluation
FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-hypopg \
    && rm -rf /var/lib/apt/lists/*

# Preload pg_stat_statements so it collects from server start.
# (hypopg does not require preloading.)
RUN echo "shared_preload_libraries = 'pg_stat_statements'" >> /usr/share/postgresql/postgresql.conf.sample \
    && echo "pg_stat_statements.max = 10000" >> /usr/share/postgresql/postgresql.conf.sample \
    && echo "pg_stat_statements.track = all" >> /usr/share/postgresql/postgresql.conf.sample
