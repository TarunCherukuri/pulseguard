from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from pendulum import datetime

PROJECT = "project-28e97307-d56d-4dc8-995"
DATASET = "pulseguard_analytics"
LOCATION = "asia-south1"

with DAG(
    dag_id="pulseguard_warehouse_refresh",
    start_date=datetime(2026, 9, 19, tz="Asia/Kolkata"),
    schedule="@hourly",
    catchup=False,
    tags=["pulseguard", "bigquery", "data-engineering"],
) as dag:

    check_source = BigQueryCheckOperator(
        task_id="check_source_data",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT}.pulseguard.transactions`
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    refresh_customer_mart = BigQueryInsertJobOperator(
        task_id="refresh_customer_anomaly_summary",
        configuration={
            "query": {
                "query": f"""
CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.customer_anomaly_summary`
AS

WITH tx_dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY validated_at DESC, kafka_offset DESC
            ) AS rn
        FROM `{PROJECT}.pulseguard.transactions` t
        WHERE event_id IS NOT NULL
    )
    WHERE rn = 1
),

tx AS (
    SELECT
        customer_id,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_amount,
        MAX(event_time) AS last_transaction_at
    FROM tx_dedup
    GROUP BY customer_id
),

anomalies AS (
    SELECT
        customer_id,
        COUNT(*) AS anomaly_count,
        MAX(detected_at) AS last_anomaly_detected_at
    FROM `{PROJECT}.pulseguard.anomalies`
    GROUP BY customer_id
)

SELECT
    tx.customer_id,
    tx.total_transactions,
    tx.total_amount,
    COALESCE(anomalies.anomaly_count, 0) AS anomaly_count,
    SAFE_DIVIDE(
        COALESCE(anomalies.anomaly_count, 0),
        tx.total_transactions
    ) AS anomaly_rate,
    tx.last_transaction_at,
    anomalies.last_anomaly_detected_at
FROM tx
LEFT JOIN anomalies
    USING (customer_id)
""",
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    check_mart = BigQueryCheckOperator(
        task_id="check_customer_mart",
        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT}.{DATASET}.customer_anomaly_summary`
        """,
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    check_source >> refresh_customer_mart >> check_mart
