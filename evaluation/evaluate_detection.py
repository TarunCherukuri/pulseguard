import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

GROUND_TRUTH_FILE = BASE_DIR / "producer" / "output" / "ground_truth.jsonl"
VALIDATED_FILE = BASE_DIR / "evaluation" / "validated-events.jsonl"
ANOMALY_FILE = BASE_DIR / "evaluation" / "anomaly-events.jsonl"

RESULTS_FILE = BASE_DIR / "evaluation" / "results.json"
INCIDENTS_FILE = BASE_DIR / "evaluation" / "incident-evaluation.jsonl"


SUPPORTED_ANOMALIES = {
    "VELOCITY_SPIKE",
    "AMOUNT_SPIKE",
    "COMBINED_ANOMALY",
}

UNSUPPORTED_ANOMALIES = {
    "NEW_DEVICE",
    "UNUSUAL_COUNTRY",
}


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_jsonl(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    raw = path.read_bytes()

    for encoding in (
        "utf-8-sig",
        "utf-16",
        "utf-16-le",
        "utf-16-be",
    ):
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

    for line_number, line in enumerate(text.splitlines(), start=1):
        line = line.strip()

        if not line:
            continue

        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            print(
                f"Skipping invalid JSON at "
                f"{path}:{line_number}: {exc}"
            )

    return records


def window_contains_event(window_start, window_end, event_time):
    return window_start <= event_time < window_end


def main():
    ground_truth_records = load_jsonl(GROUND_TRUTH_FILE)
    validated_records = load_jsonl(VALIDATED_FILE)
    anomaly_records = load_jsonl(ANOMALY_FILE)

    # ---------------------------------------------------------
    # 1. Ground truth indexed by event_id
    # ---------------------------------------------------------
    ground_truth = {
        record["event_id"]: record
        for record in ground_truth_records
        if record.get("event_id")
    }

    # ---------------------------------------------------------
    # 2. Deduplicate validated events
    # ---------------------------------------------------------
    validated = {}

    for event in validated_records:
        event_id = event.get("event_id")

        if event_id:
            validated[event_id] = event

    # ---------------------------------------------------------
    # 3. Build supported ground-truth events
    # ---------------------------------------------------------
    evaluation_events = []

    for event_id, event in validated.items():
        truth = ground_truth.get(event_id)

        if not truth:
            continue

        label = truth.get("ground_truth_anomaly", "NONE")

        if label not in SUPPORTED_ANOMALIES and label != "NONE":
            continue

        try:
            event_time = parse_timestamp(event["event_time"])
        except (KeyError, ValueError):
            continue

        incident_id = truth.get("incident_id")

        # Transactions without incident_id are treated as
        # independent incidents.
        if not incident_id:
            incident_id = f"event:{event_id}"

        evaluation_events.append(
            {
                "event_id": event_id,
                "customer_id": event.get("customer_id"),
                "event_time": event_time,
                "ground_truth": label,
                "incident_id": incident_id,
            }
        )

    # ---------------------------------------------------------
    # 4. Group events by incident
    # ---------------------------------------------------------
    incidents = {}

    for event in evaluation_events:
        incident_key = event["incident_id"]

        if incident_key not in incidents:
            incidents[incident_key] = {
                "incident_id": incident_key,
                "customer_id": event["customer_id"],
                "ground_truth": event["ground_truth"],
                "events": [],
            }

        incidents[incident_key]["events"].append(event)

    true_incidents = {
        key: value
        for key, value in incidents.items()
        if value["ground_truth"] in SUPPORTED_ANOMALIES
    }

    # ---------------------------------------------------------
    # 5. Parse anomaly windows
    # ---------------------------------------------------------
    anomaly_windows = []

    for anomaly in anomaly_records:
        customer_id = anomaly.get("customer_id")
        window_start_raw = anomaly.get("window_start")
        window_end_raw = anomaly.get("window_end")

        if not customer_id:
            continue

        if not window_start_raw or not window_end_raw:
            continue

        try:
            window_start = parse_timestamp(window_start_raw)
            window_end = parse_timestamp(window_end_raw)
        except ValueError:
            continue

        anomaly_windows.append(
            {
                "customer_id": customer_id,
                "window_start": window_start,
                "window_end": window_end,
                "anomaly_reason": anomaly.get(
                    "anomaly_reason",
                    "UNKNOWN",
                ),
                "anomaly_score": anomaly.get(
                    "anomaly_score",
                    0,
                ),
            }
        )

    # ---------------------------------------------------------
    # 6. Match anomaly windows to incidents
    # ---------------------------------------------------------
    detected_incidents = set()
    false_positive_windows = 0
    true_positive_windows = 0

    window_results = []

    for window in anomaly_windows:
        matched_incidents = []

        for incident_id, incident in true_incidents.items():
            if incident["customer_id"] != window["customer_id"]:
                continue

            for event in incident["events"]:
                if window_contains_event(
                    window["window_start"],
                    window["window_end"],
                    event["event_time"],
                ):
                    matched_incidents.append(incident_id)
                    break

        matched_incidents = sorted(set(matched_incidents))

        if matched_incidents:
            true_positive_windows += 1
            detected_incidents.update(matched_incidents)
        else:
            false_positive_windows += 1

        window_results.append(
            {
                "customer_id": window["customer_id"],
                "window_start": window["window_start"].isoformat(),
                "window_end": window["window_end"].isoformat(),
                "anomaly_reason": window["anomaly_reason"],
                "anomaly_score": window["anomaly_score"],
                "matched_incidents": matched_incidents,
            }
        )

    # ---------------------------------------------------------
    # 7. Incident-level metrics
    # ---------------------------------------------------------
    total_true_incidents = len(true_incidents)
    detected_count = len(detected_incidents)

    missed_incidents = (
        set(true_incidents.keys()) - detected_incidents
    )

    incident_recall = (
        detected_count / total_true_incidents
        if total_true_incidents
        else 0.0
    )

    # A predicted window is considered useful when it maps to
    # at least one real supported incident.
    total_predicted_windows = len(anomaly_windows)

    window_precision = (
        true_positive_windows / total_predicted_windows
        if total_predicted_windows
        else 0.0
    )

    window_recall = (
        true_positive_windows
        / (true_positive_windows + false_positive_windows)
        if (true_positive_windows + false_positive_windows)
        else 0.0
    )

    incident_f1 = (
        2 * window_precision * incident_recall
        / (window_precision + incident_recall)
        if (window_precision + incident_recall)
        else 0.0
    )

    # ---------------------------------------------------------
    # 8. Per-type results
    # ---------------------------------------------------------
    total_by_type = Counter()
    detected_by_type = Counter()

    for incident in true_incidents.values():
        anomaly_type = incident["ground_truth"]
        total_by_type[anomaly_type] += 1

    for incident_id in detected_incidents:
        anomaly_type = true_incidents[incident_id]["ground_truth"]
        detected_by_type[anomaly_type] += 1

    per_type = {}

    for anomaly_type in sorted(total_by_type):
        total = total_by_type[anomaly_type]
        detected = detected_by_type.get(anomaly_type, 0)

        per_type[anomaly_type] = {
            "total_incidents": total,
            "detected_incidents": detected,
            "missed_incidents": total - detected,
            "recall": detected / total if total else 0.0,
        }

    # ---------------------------------------------------------
    # 9. Ground-truth summary
    # ---------------------------------------------------------
    all_ground_truth_types = Counter(
        record.get("ground_truth_anomaly", "NONE")
        for record in ground_truth_records
    )

    # ---------------------------------------------------------
    # 10. Save detailed window evaluation
    # ---------------------------------------------------------
    with INCIDENTS_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        for result in window_results:
            file.write(json.dumps(result) + "\n")

    # ---------------------------------------------------------
    # 11. Save results
    # ---------------------------------------------------------
    results = {
        "evaluation_scope": {
            "validated_events": len(validated),
            "evaluated_events": len(evaluation_events),
            "supported_anomalies": sorted(SUPPORTED_ANOMALIES),
            "excluded_anomalies": sorted(UNSUPPORTED_ANOMALIES),
            "ground_truth_distribution": dict(
                all_ground_truth_types
            ),
        },
        "detection_windows": {
            "total": total_predicted_windows,
            "true_positive_windows": true_positive_windows,
            "false_positive_windows": false_positive_windows,
        },
        "incident_metrics": {
            "total_true_incidents": total_true_incidents,
            "detected_incidents": detected_count,
            "missed_incidents": len(missed_incidents),
            "window_precision": window_precision,
            "incident_recall": incident_recall,
            "incident_f1": incident_f1,
        },
        "per_type": per_type,
        "missed_incident_ids": sorted(missed_incidents),
    }

    with RESULTS_FILE.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(results, file, indent=2)

    # ---------------------------------------------------------
    # 12. Console output
    # ---------------------------------------------------------
    print("=" * 65)
    print("PulseGuard Incident-Level Detection Evaluation")
    print("=" * 65)

    print(f"Validated events       : {len(validated)}")
    print(f"Evaluated events       : {len(evaluation_events)}")
    print(f"True incidents         : {total_true_incidents}")
    print(f"Detected incidents     : {detected_count}")
    print(f"Missed incidents       : {len(missed_incidents)}")
    print()

    print(f"Anomaly windows        : {total_predicted_windows}")
    print(f"True-positive windows  : {true_positive_windows}")
    print(f"False-positive windows : {false_positive_windows}")
    print()

    print(f"Window precision       : {window_precision:.4f}")
    print(f"Incident recall        : {incident_recall:.4f}")
    print(f"Incident F1            : {incident_f1:.4f}")
    print()

    print("Per-type recall:")

    for anomaly_type, data in sorted(per_type.items()):
        print(
            f"  {anomaly_type:22s}"
            f" {data['detected_incidents']}/"
            f"{data['total_incidents']}"
            f" ({data['recall']:.4f})"
        )

    print()
    print("Excluded anomaly types:")
    for anomaly_type in sorted(UNSUPPORTED_ANOMALIES):
        print(f"  {anomaly_type}")

    print("=" * 65)
    print(f"Results saved to: {RESULTS_FILE}")
    print(f"Details saved to: {INCIDENTS_FILE}")


if __name__ == "__main__":
    main()