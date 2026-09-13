# PulseGuard

## Real-Time Payment Anomaly Detection & Incident Intelligence Platform

PulseGuard is a production-style data engineering project that
simulates payment transactions, processes them as a real-time
stream, detects behavioral anomalies, stores analytical data in
Google BigQuery, and uses a vector database to retrieve similar
historical incidents and operational runbooks.

### Synthetic anomaly scenarios

- Amount spike
- Unusual country
- New device
- Velocity spike
- Combined anomaly

## Architecture

Transaction Generator
        ↓
Apache Kafka
        ↓
PySpark Structured Streaming
        ↓
BigQuery

PySpark Anomalies
        ↓
Incident Service
        ↓
Qdrant
        ↓
Similar Historical Incidents / Runbooks

## Technology Stack

- Python
- Apache Kafka
- PySpark Structured Streaming
- Google BigQuery
- dbt
- Apache Airflow
- Qdrant
- FastAPI
- Streamlit
- Docker
- Terraform
- GitHub Actions

## Current Progress

- [x] Python environment
- [x] Project structure
- [x] Git repository
- [ ] Docker infrastructure
- [ ] Kafka
- [ ] Qdrant
- [ ] Transaction generator
- [ ] PySpark streaming
- [ ] Anomaly detection
- [ ] BigQuery
- [ ] dbt
- [ ] Airflow
- [ ] Dashboard
- [ ] Cloud deployment
- [ ] CI/CD