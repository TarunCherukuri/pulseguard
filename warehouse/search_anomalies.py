from qdrant_client import QdrantClient, models


QDRANT_URL = "http://localhost:6333"
COLLECTION = "pulseguard_anomalies"
MODEL = "BAAI/bge-small-en-v1.5"


def main():
    client = QdrantClient(url=QDRANT_URL)

    query = input("Search anomaly context: ").strip()

    if not query:
        print("Query cannot be empty.")
        return

    results = client.query_points(
        collection_name=COLLECTION,
        query=models.Document(
            text=query,
            model=MODEL,
        ),
        limit=5,
        with_payload=True,
    )

    print("\n" + "=" * 70)
    print("PulseGuard Semantic Anomaly Search")
    print("=" * 70)

    for index, result in enumerate(results.points, start=1):
        payload = result.payload or {}

        print(f"\n#{index}")
        print(f"Score          : {result.score:.4f}")
        print(f"Customer       : {payload.get('customer_id')}")
        print(f"Reason         : {payload.get('anomaly_reason')}")
        print(f"Transactions   : {payload.get('txn_count_1m')}")
        print(f"Max amount     : {payload.get('txn_amount_max_1m')}")
        print(f"Anomaly score  : {payload.get('anomaly_score')}")
        print(f"Window         : {payload.get('window_start')} -> "
              f"{payload.get('window_end')}")
        print(f"Context        : {payload.get('document')}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()