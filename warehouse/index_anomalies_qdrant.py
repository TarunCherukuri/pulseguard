import json
import hashlib
from pathlib import Path

from qdrant_client import QdrantClient, models


QDRANT_URL = "http://localhost:6333"
COLLECTION = "pulseguard_anomalies"
MODEL = "BAAI/bge-small-en-v1.5"

BASE_DIR = Path(__file__).resolve().parent.parent
ANOMALY_FILE = BASE_DIR / "evaluation" / "anomaly-events.jsonl"


def load_jsonl(path):
    raw = path.read_bytes()

    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise RuntimeError(f"Unable to decode {path}")

    return [
        json.loads(line)
        for line in text.splitlines()
        if line.strip()
    ]


def make_text(anomaly):
    return (
        f"Customer {anomaly.get('customer_id')} had "
        f"{anomaly.get('anomaly_reason')} with "
        f"{anomaly.get('txn_count_1m')} transactions in one minute. "
        f"Maximum transaction amount was "
        f"{anomaly.get('txn_amount_max_1m')}. "
        f"Average transaction amount was "
        f"{anomaly.get('txn_amount_avg_1m')}. "
        f"Anomaly score was {anomaly.get('anomaly_score')}. "
        f"Unique devices: {anomaly.get('unique_devices_1m')}. "
        f"Unique countries: {anomaly.get('unique_countries_1m')}."
    )


def point_id(anomaly):
    raw = (
        f"{anomaly.get('customer_id')}|"
        f"{anomaly.get('window_start')}|"
        f"{anomaly.get('window_end')}"
    )

    return int(hashlib.sha256(raw.encode()).hexdigest()[:15], 16)


def main():
    client = QdrantClient(url=QDRANT_URL)

    if client.collection_exists(COLLECTION):
        client.delete_collection(COLLECTION)

    vector_size = client.get_embedding_size(MODEL)

    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=models.Distance.COSINE,
        ),
    )

    anomalies = load_jsonl(ANOMALY_FILE)

    points = []

    for anomaly in anomalies:
        text = make_text(anomaly)

        points.append(
            models.PointStruct(
                id=point_id(anomaly),
                vector=models.Document(
                    text=text,
                    model=MODEL,
                ),
                payload={
                    **anomaly,
                    "document": text,
                },
            )
        )

    client.upload_points(
        collection_name=COLLECTION,
        points=points,
    )

    info = client.get_collection(COLLECTION)

    print("=" * 60)
    print("PulseGuard Qdrant Anomaly Index")
    print("=" * 60)
    print(f"Collection : {COLLECTION}")
    print(f"Model      : {MODEL}")
    print(f"Vectors    : {vector_size} dimensions")
    print(f"Indexed    : {len(points)}")
    print(f"Qdrant     : {info.status}")
    print("=" * 60)


if __name__ == "__main__":
    main()