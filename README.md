# Lambda Architecture — Bangkok Air Quality & IoT Sensor Pipeline

Course project for **2190436 Data Warehousing / 2190518 Big Data & Data Engineering (2025/2)**
at Chulalongkorn University. Solo project, May 2026.

> A production-style Lambda architecture that ingests real Bangkok air quality data and a
> synthetic high-frequency IoT sensor stream into Kafka, with a hot path for sub-minute
> anomaly detection and a cold path that builds a Kimball dimensional warehouse for
> historical analytics.

---

## Architecture

```
SOURCES                  KAFKA (KRaft, EC2)         HOT PATH
─────────────────        ──────────────────         ─────────────────────────
Air4Thai API ──────────► air-quality-raw (×3)
OpenWeather API ───────► weather-raw (×1)
Synthetic IoT gen ─────► sensor-stream  (×6) ──────► hot_path_consumer.py
                                    │                  └─► DynamoDB (TTL)
                                    │                      └─► Streamlit live dashboard
                                    │
                                    ▼ COLD PATH
                         S3 Bronze (raw Parquet)
                              │
                         AWS Glue PySpark ETL ◄── Glue Workflow (orchestrated DAG)
                              │
                         S3 Silver (cleaned, typed)
                              │
                         AWS Glue PySpark ETL
                              │
                         S3 Gold (Kimball star schema)
                              │
                    Glue Data Catalog + Athena
                              │
                         QuickSight dashboards
```

---

## Big Data Justification (3 Vs)

| Dimension | Implementation |
|---|---|
| **Volume** | Synthetic IoT stream backfilled 30 days → **25.9 M rows** in `fct_sensor_reading`; **362K anomaly events** in `fct_sensor_anomaly_event`; total Gold layer >26 M rows processed by Spark |
| **Velocity** | Hot path: Kafka → DynamoDB in **<1 minute** end-to-end; anomaly detection on every sensor tick (5-second cadence, 50 sensors) |
| **Variety** | Three heterogeneous sources: REST API with JSON (Air4Thai), REST API (OpenWeather), synthetic binary stream — different schemas, cadences, and semantics unified in one warehouse |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming bus | Apache Kafka (KRaft mode, Docker Compose on EC2) |
| Producers | Python (`confluent-kafka`), cron-scheduled on EC2 |
| Cold-path consumer | Python S3 sink consumer (Parquet, partitioned by date) |
| Hot-path consumer | Python DynamoDB writer (on-demand capacity, TTL) |
| Batch ETL | AWS Glue PySpark (9 jobs: Bronze→Silver→Gold) |
| Orchestration | AWS Glue Workflow (4-stage DAG, EventBridge→SNS failure alerts) |
| Data lake | S3 medallion architecture (Bronze / Silver / Gold) |
| Query engine | Amazon Athena (serverless, Parquet columnar) |
| BI | Amazon QuickSight (2-sheet dashboard: overview + detail) |
| Live dashboard | Streamlit (polls DynamoDB every 5 s) |
| Infrastructure | AWS EC2 t3.medium (ap-southeast-1), IAM roles, S3 lifecycle |

---

## Dimensional Model (Kimball)

### Fact tables

| Table | Grain | Rows | Source |
|---|---|---|---|
| `fct_air_quality_hourly` | One row per station per hour | 2,613 | Air4Thai |
| `fct_pollution_daily_summary` | One row per station per day | 558 | Air4Thai |
| `fct_weather_hourly` | One row per city per hour | 292 | OpenWeather |
| `fct_sensor_reading` | One row per sensor per 5-second tick | 25,920,000 | Synthetic IoT |
| `fct_sensor_anomaly_event` | One row per anomaly detected | 362,721 | Cold path reprocess of hot-path events |

### Dimensions

| Table | SCD Type | Rows |
|---|---|---|
| `dim_date` | Static | 4,018 |
| `dim_station` | **SCD2** | 187 |
| `dim_sensor` | **SCD2** | 50 |
| `dim_zone` | SCD1 | 5 |
| `dim_region` | SCD1 | — |
| `dim_weather_condition` | SCD1 | — |
| `dim_sensor_type` | SCD1 | 4 |

`dim_date` is conformed across all fact tables. `dim_station` is shared between both air-quality facts.

---

## Repo Layout

```
.
├── producers/
│   ├── air4thai_producer.py          ← real hourly PM2.5/PM10/O3 from ~100 Thai stations
│   ├── weather_producer.py           ← real hourly weather for major Thai cities
│   ├── sensor_stream_generator.py    ← synthetic IoT: 50 sensors, 5-second cadence
│   └── common/                       ← shared Kafka utils + env-var config
├── consumers/
│   ├── s3_sink_consumer.py           ← cold path: Kafka → S3 Bronze (Parquet)
│   └── hot_path_consumer.py          ← hot path: Kafka → DynamoDB + anomaly detection
├── glue_jobs/
│   ├── bronze_to_silver_air.py       ← deduplicate, type-cast, partition
│   ├── bronze_to_silver_weather.py
│   ├── bronze_to_silver_sensor.py
│   ├── silver_to_gold_dims_scd1.py   ← zone, region, weather condition, sensor type
│   ├── silver_to_gold_dim_station_scd2.py  ← SCD2 with change detection
│   ├── silver_to_gold_dim_sensor_scd2.py   ← SCD2 for sensor relocations
│   ├── silver_to_gold_facts.py       ← air quality + sensor fact tables
│   ├── silver_to_gold_fct_weather_hourly.py  ← weather fact table
│   ├── silver_to_gold_fct_sensor_anomaly.py  ← anomaly event fact (cold path closes Lambda loop)
│   └── utils/scd2_merge.py
├── infra/
│   ├── docker-compose.kafka.yml      ← Kafka KRaft single-broker
│   ├── glue_workflow_setup.py        ← idempotent boto3: workflow + triggers + SNS alerts
│   └── iam-policies/
├── dashboards/
│   └── streamlit_live_view.py        ← hot-path live anomaly dashboard
├── sql/
│   └── athena/                       ← saved business-question queries
├── tests/
│   └── test_scd2_merge.py
├── docs/
│   ├── dimensional_model.md
│   ├── architecture.md
│   ├── bronze_schema.md
│   └── glue_workflow.md
└── requirements.txt
```

---

## Running Locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in API keys + AWS config
```

Producers and consumers run on EC2 (Kafka advertised listener uses the private IP).
The Streamlit dashboard runs locally and polls DynamoDB:

```bash
streamlit run dashboards/streamlit_live_view.py
```

Glue Workflow setup (idempotent, run once per AWS account):

```bash
python3 -m infra.glue_workflow_setup
# then trigger manually:
aws glue start-workflow-run --name dw-bigdata-cold-path --region ap-southeast-1
```

---

## Key Design Decisions

- **Kafka on EC2 over MSK** — MSK costs ~$150+/month; KRaft mode on a t3.medium stays within course budget ($75 credit)
- **Custom Python consumers over Kafka Connect** — explicit offset management, clearer code ownership for a solo project
- **Glue Workflow over MWAA/Airflow** — MWAA minimum ~$300+/month; Glue Workflow is free and native to the ETL layer
- **SCD2 on dim_station and dim_sensor** — stations get recalibrated/relocated; tracking history is required for fair time-series comparison
- **DynamoDB on-demand + TTL** — zero idle cost, sub-second reads, auto-cleanup of stale hot-path records
- **All timestamps stored in UTC** — Bangkok time (UTC+7) applied only at the QuickSight presentation layer via calculated field
- **`fct_sensor_anomaly_event` reads from Gold `fct_sensor_reading`** — cold path reprocesses the full 30-day history independently from the hot path (DynamoDB), validating both paths agree (362,721 cold vs 362,734 hot — delta explained by 24hr DynamoDB TTL)
