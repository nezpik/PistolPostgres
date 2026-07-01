#!/usr/bin/env bash
# End-to-end PistolPostgres demo.
#   1. brings up Postgres (hypopg + pg_stat_statements) via docker-compose
#   2. builds the engine
#   3. seeds a synthetic edtech DB with hot, un-indexed query patterns
#   4. runs the evolution loop a few times and shows what it did
set -euo pipefail
cd "$(dirname "$0")/.."

export PISTOL_DATABASE_URL="${PISTOL_DATABASE_URL:-postgres://pistol:pistol@127.0.0.1:55432/pistol}"

echo "==> starting Postgres (hypopg + pg_stat_statements)"
docker compose up -d --build
# wait for readiness
until docker compose exec -T postgres pg_isready -U pistol -d pistol >/dev/null 2>&1; do
  sleep 1
done

echo "==> building pistol"
cargo build --release
BIN=./target/release/pistol

echo "==> initializing evolution catalog"
$BIN init

echo "==> seeding demo edtech schema + workload"
$BIN demo all --iterations 20

echo "==> running the evolution loop (3 cycles)"
for i in 1 2 3; do
  echo "----- cycle $i -----"
  $BIN run
done

echo "==> current genome / status"
$BIN status

echo "==> evolution history"
$BIN history

echo
echo "Done. Try:  $BIN propose        (see ranked proposals)"
echo "            $BIN rollback <id>   (undo a change)"
