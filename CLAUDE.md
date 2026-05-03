# CLAUDE.md

> Context file for Claude Code. Read this at the start of every session.
> Last updated: [fill in as you make changes]

---

## 1. Project Overview

**Course:** 2190436 Data Warehousing / 2190518 Big Data & Data Engineering (2025/2)
**Project:** DW/Big Data Implementation — Coexist between Traditional DE & Modern DE
**Weight:** 20% of course grade
**Team size:** Solo (1 student)
**Submission deadline:** Fri 8 May 2026 via MyCourseVille

**Personal goal beyond the grade:** This is a portfolio-grade project targeting **Streaming Data Engineer** roles. Every architectural choice should serve both the rubric AND interview-readiness. Resume-line-worthy keywords: Kafka, Spark Streaming, Lambda architecture, Kimball dimensional modeling, SCD2, Glue ETL, AWS data lake.

**One-line elevator pitch:**
> A Lambda-architecture streaming pipeline that ingests real Bangkok air quality data plus a synthetic high-frequency IoT sensor stream into Kafka, with a hot path for sub-minute anomaly detection and a cold path that builds a Kimball dimensional warehouse for historical analytics.

---

## 2. Architecture (locked decisions)

### High-level

```
SOURCES → KAFKA → ┬─ HOT PATH  → DynamoDB → live dashboard
                  └─ COLD PATH → S3 (Bronze→Silver→Gold) → Athena → QuickSight
```

### Component decisions and reasoning

| Layer | Choice | Why |
|---|---|---|
| Streaming bus | **Kafka on EC2** (KRaft mode, single broker, Docker Compose) | Industry-standard for streaming roles; MSK too expensive (~$150+/mo); reusing course lab setup |
| Ingestion | **Python producers** using `confluent-kafka` or `kafka-python` | Custom code shows understanding; no Kafka Connect dependency |
| Cold-path consumer | **Custom Python S3 sink consumer** | Simpler than Kafka Connect for solo project; clearer ownership of offsets |
| Hot-path consumer | **AWS Lambda** triggered by Kafka via MSK trigger pattern OR self-managed Python consumer on EC2 | Avoiding Glue Streaming due to cost; Spark Streaming considered, decided against |
| Data lake storage | **S3 with medallion architecture** (Bronze / Silver / Gold) | Standard pattern; Parquet partitioned by date/hour |
| Batch ETL | **AWS Glue (PySpark)** for Bronze→Silver and Silver→Gold | Course-mandated AWS service; built on Spark = Big Data justification |
| Hot-path serving | **DynamoDB** (on-demand, TTL-enabled) | Sub-second reads; cheap at low volume; auto-cleanup via TTL |
| Warehouse query | **Athena over Gold Parquet** + Glue Data Catalog | Serverless, cheap with partitioning; native QuickSight integration |
| BI | **Amazon QuickSight** (Standard, 30-day free trial) | Course-mandated; trial deliberately timed for Weeks 4-5 |
| Orchestration | **Glue Workflow** (primary) + optional **Airflow** on same EC2 (stretch) | MWAA too expensive; self-hosted Airflow only if time permits |
| Live dashboard | **Streamlit on local machine** polling DynamoDB every 5s | Cheapest, fastest demo for hot-path video |

### Big Data justification (for the rubric)

- **Volume:** Synthetic IoT sensor stream generates ~1M events/day; backfilled 30 days → ~30M rows. Cold-path Spark batch jobs process this volume.
- **Velocity:** Hot path with <1 minute end-to-end latency for anomaly detection.
- **Variety:** Mixed sources — REST API (Air4Thai), REST API (OpenWeather), synthetic high-frequency stream — all with different schemas and cadences.

### Architecture rules (do not violate)

- **EVERY producer writes to Kafka first.** No producer writes directly to S3.
- **Kafka topics are the only source of truth for inbound events.** S3 Bronze is downstream.
- **Bronze is append-only.** Silver/Gold can be overwritten.
- **No PII or credentials in any topic, S3 path, or git commit.**
- **AWS Region:** ap-southeast-1 (Singapore) — all resources must be in this region

---

## 3. Data Sources

### 3.1 Air4Thai API (real, cold path)
- **What:** Hourly PM2.5, PM10, O3, NO2, CO readings from ~100 stations across Thailand
- **Cadence:** Pull every 60 minutes
- **Topic:** `air-quality-raw` (3 partitions, key = station_id)
- **Volume estimate:** ~100 stations × 24 hr × 365 days ≈ 876k rows/year
- **Sample response:** see `docs/sample_responses/air4thai_sample.json`
- **Backfill plan:** Pull last 12-24 months of historical readings (one-time) to clear the 100k row threshold and populate the warehouse for dashboard development

### 3.2 OpenWeather API (real, cold path)
- **What:** Hourly temperature, humidity, wind, weather condition for major Thai cities
- **Cadence:** Pull every 60 minutes
- **Topic:** `weather-raw` (1 partition, key = city_id)
- **Sample response:** see `docs/sample_responses/openweather_sample.json`

### 3.3 Synthetic IoT Sensor Stream (generated, hot path)
- **Why synthetic:** Real sub-second IoT data is hard to source legally and our portfolio narrative needs a true Velocity story
- **What:** 50 simulated sensors across 5 zones in a hypothetical factory
  - 20 temperature (target 22°C, daily cycle, 1% anomaly rate)
  - 15 pressure (target 100 kPa, occasional spike anomalies)
  - 10 vibration (steady baseline, drift over hours)
  - 5 flow (correlated with pressure)
- **Cadence:** 1 reading per sensor per 5 seconds → ~864k rows/day
- **Topic:** `sensor-stream` (6 partitions, key = sensor_id)
- **Backfill mode:** generates Parquet directly to S3 Bronze for ~30 days of history in minutes

---

## 4. Dimensional Model

> Full design in `docs/dimensional_model.md`. Summary below.

### Fact tables (multiple grains, conformed dims)

| Fact table | Grain | Source |
|---|---|---|
| `fct_air_quality_hourly` | One row per station per hour | Air4Thai |
| `fct_pollution_daily_summary` | One row per station per day (different grain — proves bus matrix) | derived from above |
| `fct_sensor_reading` | One row per sensor per 5-second tick | Synthetic stream |
| `fct_sensor_anomaly_event` | One row per detected anomaly | Hot-path output, replicated to Gold for unified analytics |

### Dimensions and SCD types

| Dimension | SCD Type | Reasoning |
|---|---|---|
| `dim_date` | Static | Generated once, never changes |
| `dim_station` | **SCD2** | Stations get recalibrated, relocated; history matters for fair time-series comparison |
| `dim_sensor` | **SCD2** | Sensors get replaced/relocated; needed to demonstrate SCD2 on synthetic data too |
| `dim_zone` | SCD1 | Zones are administrative groupings; rare changes, no historical comparison need |
| `dim_region` | SCD1 | Provincial boundaries rarely change; SCD1 is sufficient |
| `dim_weather_condition` | SCD1 | Lookup table, no history needed |
| `dim_sensor_type` | SCD1 | Static-ish lookup |

### Conformed bus matrix

`dim_date` is conformed across ALL fact tables. `dim_station` is shared by both air-quality fact tables. `dim_sensor` and `dim_zone` are shared between sensor-fact tables. This conformance is what the rubric requires — do not break it.

---

## 5. Repository Layout

```
dw-bigdata-project/
├── CLAUDE.md                          ← this file
├── README.md                          ← public-facing project description
├── docs/
│   ├── architecture.md                ← architecture diagram + narrative
│   ├── dimensional_model.md           ← star schema + bus matrix + SCD decisions
│   ├── bronze_schema.md               ← per-source Bronze schema
│   ├── synthetic_data_design.md       ← sensor generator design doc
│   ├── cost-notes.md                  ← session-by-session cost log
│   └── sample_responses/              ← raw API responses for reference
├── infra/
│   ├── docker-compose.kafka.yml       ← Kafka KRaft setup
│   ├── ec2-bootstrap.sh               ← user-data script for EC2
│   └── iam-policies/                  ← JSON policy files
├── producers/
│   ├── common/
│   │   ├── kafka_utils.py             ← shared producer wrapper
│   │   └── config.py                  ← env-var-driven config
│   ├── air4thai_producer.py
│   ├── weather_producer.py
│   └── sensor_stream_generator.py     ← synthetic IoT generator (live + backfill modes)
├── consumers/
│   ├── s3_sink_consumer.py            ← cold path: Kafka → S3 Bronze (Parquet)
│   └── hot_path_consumer.py           ← hot path: Kafka → DynamoDB
├── glue_jobs/
│   ├── bronze_to_silver_air.py
│   ├── bronze_to_silver_weather.py
│   ├── bronze_to_silver_sensor.py
│   ├── silver_to_gold_dims_scd1.py
│   ├── silver_to_gold_dim_station_scd2.py
│   ├── silver_to_gold_dim_sensor_scd2.py
│   ├── silver_to_gold_facts.py
│   └── utils/
│       └── scd2_merge.py              ← reusable SCD2 merge function
├── sql/
│   ├── athena/                        ← saved business-question queries
│   ├── dim_date_generator.sql
│   └── ddl/                           ← table DDLs (if needed)
├── dashboards/
│   ├── streamlit_live_view.py         ← hot-path live dashboard
│   └── quicksight_screenshots/        ← exported PDFs/PNGs of QuickSight
├── tests/
│   └── test_scd2_merge.py             ← unit test the trickiest piece
├── requirements.txt
├── .gitignore
└── .env.example                       ← template; never commit real .env
```

---

## 6. Coding Conventions

### Python
- Python 3.11
- Black formatter, line length 100
- Type hints on every function signature
- f-strings for all string formatting
- Logging via stdlib `logging` (not `print`); root logger configured in `producers/common/config.py`
- All scripts are runnable as `python -m producers.air4thai_producer` from repo root

### PySpark / Glue
- Use `glueContext.create_dynamic_frame.from_catalog` for catalog-registered tables, raw `spark.read` for direct S3 reads
- Always specify schemas explicitly when reading; never rely on inference for production paths
- Coalesce/repartition explicitly before writing — avoid 200-tiny-files problem
- Use `s3a://` paths inside Glue/Spark, plain `s3://` paths in boto3/CLI

### Naming
- snake_case for files, functions, variables
- `dim_*` and `fct_*` for warehouse tables
- Kafka topics: kebab-case (`air-quality-raw`)
- S3 paths: lowercase, partitioned `key=value` style (`date=2026-01-15/hour=14/`)

### Timezones
- **Store everything in UTC.** No exceptions in Bronze, Silver, or Gold.
- **Convert to Asia/Bangkok ONLY at the QuickSight presentation layer**, via a calculated field.
- This is non-negotiable — past-me has been bitten by mixed timezones.

### Configuration & secrets
- All config via environment variables, loaded in `producers/common/config.py`
- `.env` file for local dev (NEVER committed)
- For AWS resources: prefer IAM roles over access keys
- API keys in AWS Secrets Manager once we move to production paths; in `.env` during local dev

---

## 7. Cost Constraints (CRITICAL — read every session)

**Budget:** $75 total AWS credit on a course-provided account.
**No Cost Explorer access** — manual tracking via `AWS_Cost_Tracker.xlsx` in Google Drive.

### Hard rules — never suggest these without explicit user approval

- ❌ **MWAA** (~$300+/mo minimum)
- ❌ **MSK / MSK Serverless** (Kafka is on EC2 instead — too expensive otherwise)
- ❌ **EMR cluster** (Glue already runs Spark; no need to double up)
- ❌ **Redshift** (we use Athena over S3 instead)
- ❌ **Kinesis** unless explicitly switching architectures (Kafka is locked in)
- ❌ **24/7 EC2 running** without justification — STOP when not in active use

### Soft rules (prefer these)

- ✅ **Glue Flex execution** for non-urgent jobs (34% cheaper)
- ✅ **Glue G.025X workers** for development if available
- ✅ **Glue job timeout = 30 minutes** during dev (prevents runaway costs)
- ✅ **DynamoDB on-demand + TTL** (auto-cleanup, no idle cost)
- ✅ **S3 lifecycle:** auto-delete Bronze older than 14 days
- ✅ **EC2 stop discipline:** target ≤8 hours/day of EC2 runtime

### Pre-implementation checklist for AWS-touching tasks

Before suggesting code that creates AWS resources, Claude Code should:
1. State the expected cost impact ("This will spin up X, costs ~$Y/hour")
2. Remind user to update `AWS_Cost_Tracker.xlsx`
3. Suggest a teardown command at the end if it's a one-shot resource

---

## 8. How to Work With Me (Claude Code, this is for you)

### Default behaviors I want

- **Explain choices, don't just write code.** When you make a non-obvious decision, tell me why in 1-2 sentences. I need to defend this in interviews.
- **Ask before architectural decisions.** If you hit a fork in the road (library choice, schema design tradeoff, naming convention), pause and ask me. Don't pick for me.
- **Use plan mode for anything touching multiple files** (Shift+Tab to enable). Show me the plan first; let me approve before you execute.
- **Critique my code.** If you see something smelly in code I wrote, point it out. Don't be polite — be useful.
- **Pin dependencies.** Every `pip install` you suggest should have an exact version (`confluent-kafka==2.5.0` not `confluent-kafka`).
- **Write tests for anything non-trivial**, especially the SCD2 merge logic.

### Things to flag immediately if I drift toward them

- 🚩 Trying to "fix" SCD2 by skipping it (it's the hardest piece AND a rubric requirement)
- 🚩 Reaching for EMR, MWAA, Redshift, MSK, Kinesis (see cost rules above)
- 🚩 Hardcoding credentials, API keys, or AWS account IDs
- 🚩 Forgetting to stop EC2 at end of a session
- 🚩 Adding new dependencies without updating `requirements.txt`
- 🚩 Writing more than ~150 lines without committing

### Commit discipline

- Commit at the end of every meaningful task
- Conventional commits format: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`
- Push to GitHub before stopping for the day (it's also a backup)

### Documentation discipline

- Every Glue job has a docstring explaining input → transform → output
- Every architectural decision goes in `docs/architecture.md` with a short reasoning paragraph
- The `dimensional_model.md` file is the single source of truth for the warehouse design — update it BEFORE changing schemas

---

## 9. Deliverables Checklist (rubric mapping)

| Rubric item | Weight | Where it lives |
|---|---|---|
| Data Collection & Source [2%] | Multiple sources, integration | `producers/`, `docs/` |
| DW & ETL [4%] | Star schema, SCD on all dims, ≥1 fact, no aggregate fact, conformed bus matrix | `glue_jobs/`, `docs/dimensional_model.md` |
| Big Data Component [4%] | Spark + Data Lake (Volume + Variety + Velocity); justification documented | `glue_jobs/`, `consumers/hot_path_consumer.py`, `docs/architecture.md` |
| Automatic Workflow [1%] | Glue Workflow on schedule; (stretch) Airflow | `infra/`, screenshots in Drive |
| BI Dashboard [4%] | ≥2 QuickSight dashboards, overview→detail flow | `dashboards/quicksight_screenshots/` |
| Presentation [5%] | 2 YouTube videos (≤15min DE, ≤10min BA) | uploaded separately, links in submission |

---

## 10. Open Questions / TODOs

> Update this section as decisions get made or open questions resolve.

- [ ] Confirm AWS budget amount with instructor (currently assuming $75)
- [ ] Decide hot-path consumer: Lambda vs self-managed EC2 Python consumer (leaning Lambda)
- [ ] Decide Airflow yes/no based on schedule progress at end of Phase 5
- [ ] Pick Air4Thai endpoint variant (depends on what historical access is available)
- [ ] Decide on QuickSight Standard vs Enterprise (Standard unless need ML insights)

---

## 11. References

- Course project PDF: `Big_Data_Implementation_2025.pdf` in repo root
- Kimball's "The Data Warehouse Toolkit" — bus matrix, SCD2 patterns
- AWS Glue docs: https://docs.aws.amazon.com/glue/
- Confluent Kafka Python docs: https://docs.confluent.io/kafka-clients/python/current/overview.html
