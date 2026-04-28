# SyncLite Consolidator — Real-Time Data Consolidation Engine

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite Consolidator?

**SyncLite Consolidator** is the central consolidation server of the SyncLite platform. It acts as the always-on sink that continuously reads log files and data streams produced by SyncLite Loggers, SyncLite DB instances, SyncLite DBReader, and SyncLite QReader, and consolidates all incoming data in real time into one or more destination databases, data warehouses, or data lakes.

It is deployed as a Java web application (WAR) on Apache Tomcat and exposes a web UI for job configuration, monitoring, and live analytics.

```
SyncLite Logger  ─┐
SyncLite DB      ─┤
SyncLite DBReader ─┤──▶  Staging Storage  ──▶  SyncLite Consolidator  ──▶  Destination(s)
SyncLite QReader ─┘
```

## Key Features

- **Real-time, transactional replication** — processes CDC log files as they arrive; sub-second latency achievable on local stages
- **Many-to-many consolidation** — one Consolidator job can aggregate data from thousands of edge devices simultaneously
- **Multiple simultaneous destinations** — fan-out a single source stream into multiple destination databases in parallel
- **Table / column / value filtering and mapping** — selectively replicate tables, rename columns, filter rows, and map data types
- **Schema evolution** — handles DDL changes (new columns, new tables) propagated from edge devices
- **Fine-tunable write modes** — INSERT, UPSERT, REPLACE, and append-only modes per table
- **Database trigger installation** — automatically installs replication triggers on destination tables
- **Built-in analytics UI** — query the destination database directly from the Consolidator web UI
- **Live dashboard** — per-device replication lag, throughput, and error tracking

## Destination Databases & Data Warehouses Supported

| Category | Systems |
|---|---|
| Relational (OLTP) | PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, DuckDB, Apache Derby, H2, HyperSQL |
| Data Warehouses | Snowflake, Google BigQuery, Amazon Redshift, ClickHouse |
| Data Lakes | Apache Iceberg, Delta Lake, Apache Hudi |
| NoSQL | MongoDB |
| File / Object | Apache Parquet, CSV on S3 / MinIO / local |

## Staging Storages Supported

SFTP · Amazon S3 · MinIO · Apache Kafka · Microsoft OneDrive · Google Drive · NFS · Local file system

## Quick Start

### Native (Windows / Ubuntu)

```bash
# From the platform release bin/ directory
./deploy.sh      # Downloads Tomcat + JDK, deploys all WARs
./start.sh       # Starts Tomcat
```

Then open: http://localhost:8080/synclite-consolidator

Default credentials (Tomcat manager): `synclite` / `synclite`

### Docker

```bash
# Edit STAGE and DST variables at the top of docker-deploy.sh first
./docker-deploy.sh   # Builds Docker image, starts Consolidator container
./docker-start.sh    # Starts the container
```

## Web UI Overview

| Page | Description |
|---|---|
| Configure Job | Wizard to set staging storage, destinations, filtering rules |
| Dashboard | Live throughput, device count, replication lag |
| List Devices | Per-device drill-down: tables replicated, lag, errors |
| Analyze Data | In-browser SQL query panel against the destination DB |
| Job Logs | Full job and error logs |

## Build

```bash
cd synclite-consolidator/root
mvn -Drevision=oss clean install
```

Built WAR: `root/web/target/synclite-consolidator-oss.war`

## Related Components

| Component | Role |
|---|---|
| [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) | Embeddable JDBC driver — produces log files on edge devices |
| [SyncLite DB](https://github.com/syncliteio/synclite-db) | HTTP server producing log files for any-language clients |
| [SyncLite DBReader](https://github.com/syncliteio/synclite-dbreader) | Reads source databases and feeds the pipeline |
| [SyncLite QReader](https://github.com/syncliteio/synclite-qreader) | Reads IoT message brokers and feeds the pipeline |
| [SyncLite Job Monitor](https://github.com/syncliteio/synclite-job-monitor) | Manages and schedules all SyncLite jobs |

## Documentation & Community

- Full documentation: https://www.synclite.io/resources/documentation
- Website: https://www.synclite.io
- Slack: https://join.slack.com/t/syncliteworkspace/shared_invite/zt-2pz945vva-uuKapsubC9Mu~uYDRKo6Jw

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)
