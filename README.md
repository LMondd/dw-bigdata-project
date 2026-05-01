# DW/Big Data Project — Streaming Air Quality + IoT Sensor Pipeline

Course project for 2190436 Data Warehousing / 2190518 Big Data & Data Engineering (2025/2).

## What this is

A Lambda-architecture data pipeline on AWS that ingests real Bangkok air quality
data plus a synthetic high-frequency IoT sensor stream into Kafka, with:

- **Hot path:** sub-minute streaming consumer for anomaly detection → DynamoDB → live dashboard
- **Cold path:** Kafka → S3 (Bronze/Silver/Gold) → Glue PySpark ETL → Kimball star schema → Athena → QuickSight

## Status

🚧 Work in progress — see `CLAUDE.md` for current architecture and decisions.

## Architecture

See [`docs/architecture.md`](docs/architecture.md).

## Repo layout

```
.
├── CLAUDE.md             ← context file for Claude Code (start here)
├── docs/                 ← architecture, dimensional model, design notes
├── infra/                ← Docker Compose, EC2 bootstrap, IAM policies
├── producers/            ← Kafka producers (Air4Thai, weather, synthetic IoT)
├── consumers/            ← Kafka consumers (S3 sink, hot-path)
├── glue_jobs/            ← PySpark Glue jobs (Bronze→Silver→Gold)
├── sql/                  ← Athena queries, DDLs, dim_date generator
├── dashboards/           ← Streamlit live view, QuickSight screenshots
└── tests/
```

## Setup

To be filled in once the project is provisioned. For now:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then fill in real values
```

## License

Educational project. Not for production use.
