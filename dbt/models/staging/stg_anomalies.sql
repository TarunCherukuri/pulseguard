SELECT
    customer_id,
    window_start,
    window_end,
    txn_count_1m,
    txn_amount_sum_1m,
    txn_amount_avg_1m,
    txn_amount_max_1m,
    txn_amount_stddev_1m,
    unique_devices_1m,
    unique_countries_1m,
    amount_to_avg_ratio,
    velocity_flag,
    amount_flag,
    anomaly_score,
    anomaly_reason,
    detected_at
FROM {{ source('pulseguard', 'anomalies') }}
WHERE customer_id IS NOT NULL