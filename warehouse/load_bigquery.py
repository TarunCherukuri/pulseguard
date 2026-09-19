import json
from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "project-28e97307-d56d-4dc8-995"
DATASET = "pulseguard"

BASE_DIR = Path(__file__).resolve().parent.parent
EVALUATION_DIR = BASE_DIR / "evaluation"


def load_jsonl(path: Path):
    raw = path.read_bytes()

    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError(
            "unknown",
            raw,
            0,
            len(raw),
            f"Unable to decode {path}",
        )

    records = []

    for line in text.splitlines():
        line = line.strip()

        if line:
            records.append(json.loads(line))

    return records


def insert_rows(client, table_name, rows):
    if not rows:
        print(f"No rows to load into {table_name}")
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(
            f"BigQuery insert failed for {table_name}: {errors}"
        )

    print(f"{table_name}: inserted {len(rows)} rows")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    validated = load_jsonl(
        EVALUATION_DIR / "validated-events.jsonl"
    )

    anomalies = load_jsonl(
        EVALUATION_DIR / "anomaly-events.jsonl"
    )

    transaction_rows = []

    for event in validated:
        transaction_rows.append(
            {
                "event_id": event.get("event_id"),
                "event_time": event.get("event_time"),
                "customer_id": event.get("customer_id"),
                "merchant_id": event.get("merchant_id"),
                "amount": event.get("amount"),
                "currency": event.get("currency"),
                "country": event.get("country"),
                "device_id": event.get("device_id"),
                "merchant_category": event.get(
                    "merchant_category"
                ),
                "channel": event.get("channel"),
                "topic": event.get("topic"),
                "kafka_partition": event.get("partition"),
                "kafka_offset": event.get("offset"),
                "kafka_timestamp": event.get(
                    "kafka_timestamp"
                ),
                "validated_at": event.get("validated_at"),
                "validation_batch_id": event.get(
                    "validation_batch_id"
                ),
            }
        )

    anomaly_rows = []

    for anomaly in anomalies:
        anomaly_rows.append(
            {
                "customer_id": anomaly.get("customer_id"),
                "window_start": anomaly.get("window_start"),
                "window_end": anomaly.get("window_end"),
                "txn_count_1m": anomaly.get("txn_count_1m"),
                "txn_amount_sum_1m": anomaly.get(
                    "txn_amount_sum_1m"
                ),
                "txn_amount_avg_1m": anomaly.get(
                    "txn_amount_avg_1m"
                ),
                "txn_amount_max_1m": anomaly.get(
                    "txn_amount_max_1m"
                ),
                "txn_amount_stddev_1m": anomaly.get(
                    "txn_amount_stddev_1m"
                ),
                "unique_devices_1m": anomaly.get(
                    "unique_devices_1m"
                ),
                "unique_countries_1m": anomaly.get(
                    "unique_countries_1m"
                ),
                "amount_to_avg_ratio": anomaly.get(
                    "amount_to_avg_ratio"
                ),
                "velocity_flag": anomaly.get("velocity_flag"),
                "amount_flag": anomaly.get("amount_flag"),
                "anomaly_score": anomaly.get("anomaly_score"),
                "anomaly_reason": anomaly.get(
                    "anomaly_reason"
                ),
                "detected_at": anomaly.get("detected_at"),
            }
        )

    insert_rows(client, "transactions", transaction_rows)
    insert_rows(client, "anomalies", anomaly_rows)

    print("BigQuery loading complete.")


if __name__ == "__main__":
    main()