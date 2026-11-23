# DBLSSIS_ORACLE_ETL

SSIS ETL that loads dyeing batch records from SQL Server (`DBL_DIGITAL.dbo.ETL_Batch_SQL_STG`) into Oracle E-Business Suite (`APPS.ETL_BATCH_SQL_STG`) every 30 minutes.

| | |
|---|---|
| **Engine** | SQL Server 2019 Integration Services, project deployment model, SSISDB catalog |
| **Target** | Oracle Database 23ai family through OraOLEDB (Oracle Client 19c x64) |
| **Load pattern** | Incremental append on business key `BatchNO`; changed rows reported, never overwritten |
| **Schedule** | SQL Server Agent job `DBL_ETL_BATCH_SQL_STG_30MIN`: (1) source refresh procedure, then (2) the SSIS package |
| **Status** | Deployed on DEV. Initial and incremental loads reconciled: 47/47 metrics match |

## How it works

```
every 30 min ─► Step 1  EXEC dbo.SP_ETL_Batch_SQL_STG     (refresh source staging)
                Step 2  ETL_BATCH_SQL_STG.dtsx (SSISDB)
                          ├─ pre-check: no duplicate keys on either side
                          ├─ Lookup on BATCHNO in Oracle
                          │    ├─ new        → INSERT into Oracle
                          │    ├─ unchanged  → skip
                          │    └─ changed    → Pending_Corrections CSV (no update)
                          └─ post-check: row conservation + target delta + zero rejects
```

Any failed check stops the run with a diagnostic error. Re-running is safe: only missing keys are inserted.

## Documentation

**[Technical documentation](docs/TECHNICAL_DOCUMENTATION.md)** covers architecture, column mapping, package design, quality gates, security, deployment, scheduling, monitoring, troubleshooting and open risks.

## Repository layout

| Path | Content |
|---|---|
| `src/DBLSSIS_ORACLE_ETL/` | Visual Studio SSIS project and package |
| `deployment/` | Agent job creation, procedure step, monitoring query |
| `validation/` | Read-only profiling, readiness and 47-metric reconciliation scripts |
| `tools/package_generator/` | Programmatic package builder (SSIS object model) |

## Quick start

1. Clone the repository. In the two `.conmgr` files, replace the placeholder tokens (`SQLSERVER_HOST`, `ORACLE_HOST`, …) **in a private working copy**. See [token list](docs/TECHNICAL_DOCUMENTATION.md#17-sanitization-tokens).
2. Build in Visual Studio 2022 with the SSIS Projects extension and deploy the `.ispac` to SSISDB.
3. Configure environment `DEV` with sensitive password variables ([§9](docs/TECHNICAL_DOCUMENTATION.md#9-deployment-ssisdb)).
4. Run `deployment/create_agent_job_30min.sql`, then `deployment/add_proc_step_to_job.sql`.
5. Monitor with `deployment/monitor_etl_job.sql`.

## Security

This repository contains **no credentials**. Passwords are supplied at runtime from sensitive SSISDB environment variables. Server addresses and logins are replaced with tokens. Never commit real connection details, `.ispac` files, or validation output files.
