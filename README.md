# pgflow

Native PostgreSQL DAG orchestration engine (Airflow/dbt-like) built entirely in SQL/PLpgSQL, with built-in observability layers (Medallion: Bronze/Silver/Gold) and semantic reporting.

## TL;DR

- **Runs DAG orchestration inside PostgreSQL** (no external Python scheduler required).
- **Supports dependencies, retries, scheduling, async workers, and catchup/backfill**.
- **Includes observability and SLA analytics** in `dag_medallion` and `dag_semantic`.
- **Comes with demo domains** (`varejo`, `biblioteca`, `rede_social`) and validation scripts.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Core Schemas](#core-schemas)
- [Deploy and Run a DAG](#deploy-and-run-a-dag)
- [Operations and Monitoring](#operations-and-monitoring)
- [Validation / Testing](#validation--testing)
- [Async Execution Notes](#async-execution-notes)
- [Important Caveats](#important-caveats)
- [Development Workflow](#development-workflow)
- [License](#license)

---

## Architecture Overview

`pgflow` is centered around three schema layers:

1. **`dag_engine`**  
   Core orchestration engine:
   - DAG/task metadata
   - dependency graph management (cycle prevention)
   - execution state transitions (`PENDING` → `RUNNING` → `SUCCESS` / `FAILED`)
   - scheduling and execution procedures
   - async worker dispatch (`pg_background` preferred, `dblink` fallback)

2. **`dag_medallion`**  
   Observability model:
   - **Bronze**: raw execution/state snapshots
   - **Silver**: normalized dimensions/facts (`dim_task`, `dim_run`, `fato_task_exec`)
   - **Gold**: SLA, critical-path, and health-oriented analytics

3. **`dag_semantic`**  
   Reporting/consumption layer for operational and management views.

---

## Repository Structure

```text
.
├── docker-compose.yaml         # PostgreSQL runtime environment
├── pgflow_engine.sql           # Main engine + medallion + semantic SQL objects
└── scripts/
    ├── setup_database.sql      # Creates/initializes demo schemas
    ├── setup_varejo.sql        # Retail domain sample setup
    └── validate_all.sh         # Parallelized SQL validation script
```

---

## Prerequisites

- Docker + Docker Compose
- PostgreSQL with support for:
  - `pg_cron` (required for scheduling)
  - `dblink` (required async fallback)
  - `pg_background` (optional, preferred async worker path)

> `pg_cron` must be present in `shared_preload_libraries` in `postgresql.conf`.

---

## Quick Start

### 1) Start PostgreSQL container

```bash
docker-compose up -d
```

From `docker-compose.yaml`:
- Container: `curso_modelagem_postgres`
- Database: `curso_modelagem`
- User: `aluno`
- Password: `modelagem_password`

### 2) Initialize database objects

Load SQL files into the running database (example with `psql`):

```bash
psql -h localhost -U aluno -d curso_modelagem -f pgflow_engine.sql
psql -h localhost -U aluno -d curso_modelagem -f scripts/setup_database.sql
```

### 3) Validate setup

```bash
./scripts/validate_all.sh
```

---

## Core Schemas

### Engine/analytics schemas
- `dag_engine`
- `dag_medallion`
- `dag_semantic`

### Demo domain schemas
- `varejo` (retail)
- `biblioteca` (library)
- `rede_social` (social network)

---

## Deploy and Run a DAG

### Declarative DAG deployment

```sql
CALL dag_engine.proc_deploy_dag('{
  "name": "daily_varejo_dw",
  "description": "Carga D-1 completa das Fatos e Dimensões do Varejo",
  "schedule": "0 2 * * *",
  "tasks": [
    {
      "task_name": "1_snapshot_clientes",
      "call_fn": "varejo.proc_snapshot_clientes",
      "call_args": ["$1"],
      "medallion_layer": "BRONZE",
      "dependencies": [],
      "max_retries": 0
    }
  ]
}'::JSONB, 'Initial deploy');
```

### Run a specific execution date

```sql
CALL dag_engine.proc_run_dag('daily_varejo_dw', '2024-05-04');
```

### Run catchup/backfill

```sql
CALL dag_engine.proc_catchup('daily_varejo_dw', '2024-05-06', '2024-07-04');
```

---

## Operations and Monitoring

Useful operational calls/queries:

### Clear a run

```sql
CALL dag_engine.proc_clear_run('daily_varejo_dw', '2024-05-04');
```

### Render DAG dependency graph (Mermaid)

```sql
SELECT dag_engine.fn_dag_to_mermaid('daily_varejo_dw');
```

### Engine percentile performance view

```sql
SELECT *
FROM dag_engine.v_task_percentiles
ORDER BY p99_ms DESC;
```

### Basic task metadata check

```sql
SELECT * FROM dag_engine.tasks;
```

---

## Validation / Testing

Primary validation path:

```bash
./scripts/validate_all.sh
```

This script runs domain/schema validations in parallel and is the fastest smoke-test for local changes.

---

## Async Execution Notes

Execution model:
- Prefer **`pg_background`** when available.
- Fallback to **`dblink`** async execution when `pg_background` is unavailable.

If running with `dblink`, ensure worker DSN is configured in:
- `dag_engine.worker_dsn`

---

## Important Caveats

- Some procedures use internal `COMMIT` for orchestration/worker control.
  - Do **not** call these inside outer transaction blocks.
- Cycle-prevention logic is enforced by trigger (`trg_check_cycles`) to block circular dependencies.
- Scheduling behavior depends on correct `pg_cron` environment/configuration.

---

## Development Workflow

1. Modify SQL objects in `pgflow_engine.sql` and/or domain scripts in `scripts/`.
2. Reapply SQL to local PostgreSQL.
3. Run validation script:
   ```bash
   ./scripts/validate_all.sh
   ```
4. Verify operational views/procedures with sample DAG runs.

---

## License

MIT License  
Copyright (c) 2026 Bruno Peixoto
