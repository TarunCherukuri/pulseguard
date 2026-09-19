SELECT
    customer_id,
    COUNT(*) AS anomaly_count,
    COUNTIF(anomaly_reason = 'VELOCITY_SPIKE') AS velocity_anomalies,
    COUNTIF(anomaly_reason = 'AMOUNT_SPIKE') AS amount_anomalies,
    COUNTIF(anomaly_reason = 'VELOCITY_AND_AMOUNT_SPIKE') AS combined_anomalies,
    MAX(anomaly_score) AS max_anomaly_score,
    MAX(txn_amount_max_1m) AS max_transaction_amount,
    MAX(txn_count_1m) AS max_transactions_per_minute,
    MAX(detected_at) AS last_detected_at
FROM {{ ref('stg_anomalies') }}
GROUP BY customer_id