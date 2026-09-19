# PulseGuard

## Streaming Transaction Anomaly Detection Platform

PulseGuard is an end-to-end data engineering and anomaly detection platform designed to process transaction events in near real time.

The system combines Kafka, PySpark Structured Streaming, BigQuery, dbt, Airflow, Qdrant, and Power BI into a production-style streaming analytics pipeline.

---

## Architecture

```text
Transaction Generator
        |
        v
      Kafka
        |
        v
PySpark Structured Streaming
        |
        +--------------------+
        |                    |
        v                    v
Validation +           Feature Engineering
Data Quality                 |
        |                    v
        v              Anomaly Detection
Validated Kafka Topic        |
        |                    v
        |              Anomaly Kafka Topic
        |                    |
        +----------+---------+
                   |
                   v
                BigQuery
                   |
                   v
                  dbt
                   |
                   v
            Analytics Marts
                   |
            +------+------+
            |             |
            v             v
        Power BI       Qdrant
        Dashboard      Semantic Search

Airflow orchestrates warehouse refresh workflows.

## Author

Tarun Sai Teja Cherukuri

Data Engineer | Backend Engineer | Applied AI

Bengaluru, India
