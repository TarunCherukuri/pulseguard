# PulseGuard

## Real-Time Transaction Anomaly Detection & Analytics Platform

PulseGuard is an end-to-end streaming data engineering platform designed to detect suspicious transaction behavior in near real time.

The platform ingests transaction events through **Apache Kafka**, processes and validates them using **PySpark Structured Streaming**, performs window-based anomaly detection, stores operational data in **Google BigQuery**, transforms warehouse data using **dbt**, orchestrates warehouse workflows with **Apache Airflow**, and enables semantic anomaly investigation through **Qdrant** and vector embeddings.

A **Power BI dashboard** provides analytical views over customers, transactions, anomalies, and anomaly patterns.

The project demonstrates production-oriented data engineering concepts including streaming ingestion, data quality, stateful processing, event lineage, deduplication, warehouse modeling, orchestration, observability, semantic search, and quantitative anomaly evaluation.

---

## Architecture

```text
                         +----------------------+
                         | Transaction Generator|
                         +----------+-----------+
                                    |
                                    v
                          +------------------+
                          |      Kafka       |
                          | transaction-     |
                          | events           |
                          +--------+---------+
                                   |
                                   v
                   +-----------------------------+
                   | PySpark Structured Streaming|
                   +-------------+---------------+
                                 |
                    +------------+------------+
                    |                         |
                    v                         v
           +------------------+      +-------------------+
           | Validation &     |      | Feature Engineering|
           | Data Quality     |      | & Windowing        |
           +--------+---------+      +---------+----------+
                    |                          |
                    v                          v
           +------------------+      +-------------------+
           | Quarantine       |      | Anomaly Detection |
           | Invalid Events   |      +---------+---------+
           +------------------+                |
                                               v
                                   +----------------------+
                                   | anomaly-events Kafka |
                                   +----------+-----------+
                                              |
                         +--------------------+--------------------+
                         |                                         |
                         v                                         v
                +-------------------+                       +-------------+
                |    BigQuery       |                       |   Qdrant   |
                | Transactions      |                       | Vector DB  |
                | Anomalies         |                       +------+------+
                | Validation Events |                              |
                +---------+---------+                              v
                          |                                Semantic Search
                          v
                    +-----------+
                    |    dbt    |
                    +-----+-----+
                          |
                          v
                  Analytics Marts
                          |
                          v
                    +-----------+
                    | Power BI  |
                    +-----------+

              Apache Airflow orchestrates warehouse workflows.
```

---

## Key Capabilities

### Real-Time Streaming

- Apache Kafka for event ingestion
- PySpark Structured Streaming for continuous processing
- Stateful time-window aggregations
- Event-time watermarking
- Separate Kafka topics for raw, validated, and anomalous events

### Data Quality & Validation

Incoming events are validated before entering the analytical pipeline.

Validation checks include:

- Missing or blank event identifiers
- Invalid timestamps
- Missing customer or merchant identifiers
- Invalid transaction amounts
- Unsupported currencies
- Missing country, device, or merchant category
- Invalid transaction channels
- Malformed JSON payloads

Invalid events are written to a quarantine location instead of being silently discarded.

### Streaming Anomaly Detection

PulseGuard detects behavioral anomalies using customer-level transaction features calculated over time windows.

Current signals include:

- Transaction velocity
- Transaction amount spikes
- Amount-to-average transaction ratio
- Combined anomaly conditions

Example:

```text
Customer
   |
   +-- Transaction count
   |
   +-- Average transaction amount
   |
   +-- Maximum transaction amount
   |
   +-- Device diversity
   |
   +-- Country diversity
   |
   v
Anomaly Features
   |
   v
Rule-Based Detection
   |
   v
Anomaly Score + Reason
```

Current anomaly reasons:

```text
VELOCITY_SPIKE
AMOUNT_SPIKE
COMBINED_ANOMALY
```

The synthetic data generator also supports:

```text
NEW_DEVICE
UNUSUAL_COUNTRY
```

These scenarios are currently available for evaluation data generation but are not implemented by the active streaming detector.

---

## Kafka Topics

| Topic | Purpose |
|---|---|
| `transaction-events` | Raw transaction events |
| `validated-transaction-events` | Events that pass validation |
| `anomaly-events` | Detected anomalous transaction windows |

Validated records retain Kafka lineage information including:

- Topic
- Partition
- Offset
- Kafka timestamp
- Validation timestamp
- Validation batch ID

This provides traceability from downstream records back to the original Kafka event.

---

## Data Processing Flow

### 1. Transaction Generation

`producer/transaction_generator.py` generates synthetic transaction events representing normal and anomalous customer behavior.

Supported scenarios include:

- Normal transactions
- High transaction velocity
- High transaction amounts
- New devices
- Unusual countries
- Combined anomaly scenarios

Ground-truth anomaly information is recorded separately for evaluation.

---

### 2. Kafka Ingestion

Generated transactions are published to:

```text
transaction-events
```

Kafka provides the event streaming layer between the transaction generator and downstream processing jobs.

---

### 3. Streaming Validation

`streaming/kafka_stream.py` consumes raw Kafka events and performs schema parsing and data-quality validation.

Valid events are published to:

```text
validated-transaction-events
```

Invalid events are written to:

```text
quarantine/invalid_events
```

Validated records include Kafka lineage metadata so individual events can be traced through the pipeline.

---

### 4. Feature Engineering

`streaming/feature_stream.py` performs stateful customer-level aggregations using:

- 1-minute windows
- 10-second sliding intervals
- 2-minute event-time watermarking

Features include:

```text
transaction count
transaction sum
average transaction amount
maximum transaction amount
transaction amount standard deviation
device diversity
country diversity
```

These features are used to derive anomaly signals.

---

### 5. Anomaly Detection

`streaming/anomaly_stream.py` identifies windows where anomaly conditions are satisfied.

Example rules include:

```text
Velocity:
transaction_count >= 6

Amount:
max transaction amount >= 100000
OR
maximum amount / average amount >= 5
```

The detector generates an anomaly score and reason before publishing the result to:

```text
anomaly-events
```

---

# BigQuery Warehouse

PulseGuard uses **Google BigQuery** as the analytical warehouse.

Primary dataset:

```text
pulseguard
```

Operational tables:

```text
transactions
anomalies
validation_events
```

The tables use partitioning and clustering for analytical workloads.

### Transactions

```text
Partitioned by: event_time
Clustered by: customer_id, merchant_id
```

### Anomalies

```text
Partitioned by: detected_at
Clustered by: customer_id, anomaly_reason
```

### Validation Events

```text
Partitioned by: validated_at
Clustered by: is_valid, validation_reason
```

A Python Kafka-to-BigQuery consumer continuously loads streaming results into the warehouse.

---

# dbt Transformation Layer

The `dbt/` directory contains the warehouse transformation layer.

### Staging Models

```text
stg_transactions
stg_anomalies
```

The transaction staging model performs event-level deduplication using Kafka lineage information.

Example:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY validated_at DESC, kafka_offset DESC
) = 1
```

This prevents duplicate events from propagating into downstream analytical models.

### Analytics Mart

```text
customer_anomaly_summary
```

The customer-level mart provides:

- Transaction count
- Transaction value
- Anomaly count
- Anomaly rate
- Last transaction timestamp

The mart is used by the Power BI dashboard and warehouse workflows.

---

# Airflow Orchestration

Apache Airflow orchestrates the warehouse refresh workflow.

DAG:

```text
pulseguard_warehouse_refresh
```

Workflow:

```text
Source Data Check
       |
       v
Refresh Customer Anomaly Summary
       |
       v
Customer Mart Validation
```

The DAG performs data-quality checks before and after the analytical mart refresh.

---

# Qdrant Semantic Search

PulseGuard includes a semantic search layer for anomaly investigation.

Anomaly records are converted into text descriptions and embedded using:

```text
BAAI/bge-small-en-v1.5
```

Embeddings are stored in:

```text
Qdrant
```

Collection:

```text
pulseguard_anomalies
```

Vector dimension:

```text
384
```

This allows users to search anomalies using natural language.

Example:

```text
customer showing unusually high transaction velocity
```

Search flow:

```text
User Query
    |
    v
Embedding Model
    |
    v
Qdrant Vector Search
    |
    v
Similar Anomaly Records
```

---

# Evaluation

PulseGuard includes an incident-level evaluation framework in:

```text
evaluation/evaluate_detection.py
```

The evaluation compares detected anomaly windows with known ground-truth incidents generated by the transaction simulator.

Metrics include:

- Window-level precision
- Incident-level recall
- Incident-level F1
- Detection performance by anomaly type

A baseline evaluation run produced:

| Metric | Result |
|---|---:|
| Validated events | 5,000 |
| Evaluated events | 4,842 |
| Ground-truth incidents | 158 |
| Detected incidents | 25 |
| Anomaly windows | 252 |
| True-positive windows | 136 |
| False-positive windows | 116 |
| Window precision | 0.5397 |
| Incident recall | 0.1582 |
| Incident F1 | 0.2447 |

Per-type incident detection:

| Anomaly Type | Detected / Actual |
|---|---:|
| `AMOUNT_SPIKE` | 8 / 61 |
| `COMBINED_ANOMALY` | 13 / 67 |
| `VELOCITY_SPIKE` | 4 / 30 |

`NEW_DEVICE` and `UNUSUAL_COUNTRY` were excluded from detector-performance metrics because the current streaming detector does not implement those signals.

These results represent a baseline for the current rule-based detector and are not intended as a production fraud-detection benchmark.

---

# Observability

PulseGuard includes:

```text
observability/health_check.py
```

The health check validates:

- Kafka connectivity
- Kafka topic availability
- Qdrant connectivity
- Spark availability
- Kafka container status
- Qdrant container status
- Spark container status
- Airflow scheduler status
- BigQuery connectivity
- Warehouse record counts
- Customer mart availability

The check produces a machine-readable runtime result for operational diagnostics.

---

# Power BI Dashboard

The Power BI dashboard uses BigQuery analytical data to monitor:

- Total customers
- Total transactions
- Total transaction value
- Total anomalies
- Average anomaly rate
- Top customers by anomaly count
- Customer anomaly rate
- Anomaly reason distribution
- Anomaly detection trend
- Customer and anomaly filters

Dashboard file:

```text
dashboard/PulseGuard.pbix
```

---

# Project Structure

```text
pulseguard/
│
├── airflow/
│   ├── dags/
│   │   └── pulseguard_warehouse.py
│   └── docker-compose.pulseguard.yaml
│
├── dashboard/
│   └── PulseGuard.pbix
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_transactions.sql
│   │   │   └── stg_anomalies.sql
│   │   └── marts/
│   │       └── customer_anomaly_summary.sql
│   └── dbt_project.yml
│
├── evaluation/
│   ├── create_transactions.sql
│   ├── evaluate_detection.py
│   ├── incident-evaluation.jsonl
│   └── results.json
│
├── observability/
│   └── health_check.py
│
├── producer/
│   ├── transaction_generator.py
│   └── data/
│       └── customers.json
│
├── streaming/
│   ├── kafka_stream.py
│   ├── feature_stream.py
│   └── anomaly_stream.py
│
├── warehouse/
│   ├── load_bigquery.py
│   ├── kafka_to_bigquery.py
│   ├── index_anomalies_qdrant.py
│   └── search_anomalies.py
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── .gitignore
```

---

# Technology Stack

| Area | Technology |
|---|---|
| Event Streaming | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Programming | Python |
| Data Warehouse | Google BigQuery |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Vector Database | Qdrant |
| Embeddings | BAAI/bge-small-en-v1.5 |
| BI / Visualization | Power BI |
| Containers | Docker / Docker Compose |
| Cloud Platform | Google Cloud |
| Version Control | Git / GitHub |

---

# Engineering Practices

### Data Quality

Invalid events are explicitly identified and quarantined rather than silently dropped.

### Event Lineage

Kafka topic, partition, offset, timestamps, and validation metadata are retained to improve traceability.

### Stateful Streaming

The anomaly detector uses event-time windows and watermarks rather than evaluating events only in isolation.

### Deduplication

Warehouse transformations use event-level deduplication to reduce duplicate records in downstream analytics.

### Separation of Concerns

Streaming ingestion, validation, feature engineering, anomaly detection, warehousing, transformation, orchestration, and visualization are implemented as separate components.

### Observability

Infrastructure and data-path health checks provide operational visibility into the pipeline.

### Reproducible Evaluation

Synthetic transactions are generated with known ground-truth incidents so anomaly detection can be measured quantitatively.

---

# Running PulseGuard

## Start Infrastructure

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

---

## Start Transaction Generator

Activate the virtual environment or use the project Python executable:

```powershell
.\.venv\Scripts\python.exe producer\transaction_generator.py
```

Transactions will be published to:

```text
transaction-events
```

---

## Run Streaming Jobs

The PySpark streaming jobs run inside the Spark container.

Example pattern:

```powershell
docker exec pulseguard-spark /opt/spark/bin/spark-submit ...
```

The required Kafka connector and Spark configuration depend on the specific streaming job being started.

---

# Configuration & Credentials

Local credentials and environment-specific configuration should never be committed to Git.

The repository ignores sensitive and runtime files including:

```text
.env
credentials files
service-account files
application_default_credentials.json
runtime checkpoints
quarantine output
generated logs
```

Google Cloud authentication for local development uses Application Default Credentials.

---

# Future Improvements

Potential extensions to the current implementation include:

- Machine-learning-based anomaly scoring
- `NEW_DEVICE` detection
- `UNUSUAL_COUNTRY` detection
- Real-time validation metrics
- Kafka schema management
- Stronger exactly-once and idempotent sink guarantees
- Automated model evaluation pipelines
- High-severity anomaly alerting
- Expanded semantic investigation workflows
- CI/CD for streaming, dbt, and Airflow components

---

# Author

**Tarun Sai Teja Cherukuri**

Data Engineer | Backend Engineer | Applied AI

India

GitHub: [TarunCherukuri](https://github.com/TarunCherukuri)