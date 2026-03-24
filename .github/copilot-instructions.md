# Copilot Instructions — ms-fabric-jump-start

## Project Overview

This repository contains Microsoft Fabric resources and samples, including:

- **Fabric Notebooks** — PySpark and Python notebooks for data engineering and data science workloads
- **Data Pipelines & Dataflows** — Orchestration and ETL/ELT definitions
- **Lakehouse / Warehouse SQL** — SQL scripts for Lakehouse and Synapse Data Warehouse
- **Power BI** — Report definitions and semantic models
- **Documentation** — Guides and walkthroughs for getting started with Fabric

## Architecture

Resources are organized by Fabric workload type. Notebooks are the primary artifact and typically target Fabric's Spark runtime (Spark 3.4+ with Delta Lake). Pipelines and dataflows reference notebooks or SQL scripts for execution.

## Conventions

### Notebooks

- Use PySpark as the default language; use `%%sql` magic for inline SQL cells
- Prefer Delta Lake format for all table reads/writes (`spark.read.format("delta")`)
- Use Fabric-native APIs (`mssparkutils`, `notebookutils`) for secrets, file operations, and lakehouse mounts — do not use Databricks-specific `dbutils`
- Structure notebooks with markdown header cells explaining each step
- Use relative paths via the default lakehouse (`Files/` and `Tables/`) rather than hardcoded abfss:// URIs

### SQL Scripts

- Target T-SQL dialect for Warehouse, Spark SQL for Lakehouse
- Use schema-qualified table names (e.g., `dbo.table_name`)

### Power BI

- Semantic models should use import mode unless the sample specifically demonstrates DirectLake
- Include sample data or instructions for generating it

### General

- Each sample should be self-contained with its own README explaining prerequisites and setup steps
- Use English for all code comments and documentation
