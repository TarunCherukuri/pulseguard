import json
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path

RESULT = {
    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "checks": {},
}


def check_port(name, host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            return {"status": "PASS", "detail": f"{host}:{port} reachable"}
    except Exception as exc:
        return {"status": "FAIL", "detail": str(exc)}


def check_docker_container(name):
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        status = result.stdout.strip()

        if result.returncode == 0 and status:
            return {
                "status": "PASS" if status == "running" else "FAIL",
                "detail": status,
            }

        return {"status": "FAIL", "detail": "container not found"}
    except Exception as exc:
        return {"status": "FAIL", "detail": str(exc)}


def check_kafka():
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        metadata = admin.list_topics(timeout=5)

        topics = sorted(
            topic
            for topic in metadata.topics
            if not topic.startswith("__")
        )

        required = {
            "transaction-events",
            "validated-transaction-events",
            "anomaly-events",
        }

        missing = sorted(required - set(topics))

        return {
            "status": "PASS" if not missing else "FAIL",
            "topics": topics,
            "missing_topics": missing,
        }
    except Exception as exc:
        return {"status": "FAIL", "detail": str(exc)}


def check_qdrant():
    try:
        from qdrant_client import QdrantClient

        client = QdrantClient(url="http://localhost:6333", timeout=5)
        collections = client.get_collections()

        names = sorted(c.name for c in collections.collections)

        return {
            "status": "PASS" if "pulseguard_anomalies" in names else "WARN",
            "collections": names,
        }
    except Exception as exc:
        return {"status": "FAIL", "detail": str(exc)}


def check_bigquery():
    try:
        from google.cloud import bigquery

        project = "project-28e97307-d56d-4dc8-995"
        client = bigquery.Client(project=project)

        sql = f"""
        SELECT
            (SELECT COUNT(*) FROM `{project}.pulseguard.transactions`) AS transactions,
            (SELECT COUNT(*) FROM `{project}.pulseguard.anomalies`) AS anomalies,
            (SELECT COUNT(*) FROM `{project}.pulseguard.validation_events`) AS validation_events,
            (SELECT COUNT(*) FROM `{project}.pulseguard_analytics.customer_anomaly_summary`) AS customers,
            (SELECT MAX(detected_at) FROM `{project}.pulseguard.anomalies`) AS latest_anomaly
        """

        row = next(iter(client.query(sql).result()))

        return {
            "status": "PASS",
            "transactions": row.transactions,
            "anomalies": row.anomalies,
            "validation_events": row.validation_events,
            "customers": row.customers,
            "latest_anomaly": (
                row.latest_anomaly.isoformat()
                if row.latest_anomaly else None
            ),
        }
    except Exception as exc:
        return {"status": "FAIL", "detail": str(exc)}


RESULT["checks"]["kafka_port"] = check_port("Kafka", "localhost", 9092)
RESULT["checks"]["qdrant_port"] = check_port("Qdrant", "localhost", 6333)
RESULT["checks"]["spark_port"] = check_port("Spark UI", "localhost", 4040)

RESULT["checks"]["kafka_container"] = check_docker_container("pulseguard-kafka")
RESULT["checks"]["qdrant_container"] = check_docker_container("pulseguard-qdrant")
RESULT["checks"]["spark_container"] = check_docker_container("pulseguard-spark")
RESULT["checks"]["airflow_scheduler"] = check_docker_container(
    "airflow-airflow-scheduler-1"
)

RESULT["checks"]["kafka"] = check_kafka()
RESULT["checks"]["qdrant"] = check_qdrant()
RESULT["checks"]["bigquery"] = check_bigquery()

Path("observability").mkdir(exist_ok=True)

with open("observability/latest_health.json", "w", encoding="utf-8") as f:
    json.dump(RESULT, f, indent=2, default=str)

print(json.dumps(RESULT, indent=2, default=str))

failed = [
    name
    for name, check in RESULT["checks"].items()
    if check.get("status") == "FAIL"
]

raise SystemExit(1 if failed else 0)
