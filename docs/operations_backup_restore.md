# Operations Backup And Restore

This project keeps operational paper and live state in PostgreSQL. Backups are part of the runtime contract before any sustained paper burn-in or live promotion.

## What Must Be Backed Up

- `strategy_runs`
- `signals`
- `target_weights`
- `target_positions`
- `risk_checks`
- `orders`
- `order_events`
- `fills`
- `positions`
- `account_snapshots`
- `pnl_snapshots`
- `risk_snapshots`
- `incidents`

Market-data tables should also be backed up when they are the only canonical copy in the environment.

## Minimum Backup Policy

- take a PostgreSQL backup before schema migrations in any non-dev environment
- take at least one daily backup during paper or live operation
- retain multiple restore points, not just the latest dump
- store backups outside the running container filesystem
- verify restores regularly on a disposable database before trusting the policy

## Local Backup Command

With the local Compose PostgreSQL running:

```bash
docker compose exec -T postgres pg_dump -U quant -d quant_core --format=custom --file=/tmp/quant_core.dump
docker compose cp postgres:/tmp/quant_core.dump ./tmp/quant_core.dump
```

If `./tmp` does not exist yet, create it first or copy to another repo-local path.

## Local Restore Command

Restore into a fresh database, not on top of a running operational database:

```bash
docker compose exec -T postgres createdb -U quant quant_core_restore
cat ./tmp/quant_core.dump | docker compose exec -T postgres pg_restore -U quant -d quant_core_restore --clean --if-exists
```

## Restore Checklist

After restore, verify:

- schema migrated cleanly
- latest `strategy_runs` rows exist
- latest `orders`, `fills`, and `incidents` rows exist
- latest `positions`, `account_snapshots`, and `risk_snapshots` rows exist
- paper-run reports are present in `strategy_runs.metadata_json`

## Recovery Rule

If a restore does not reproduce the latest expected operational state, do not resume paper or live runs until the gap is understood. Backups are only useful when restore quality is proven.
