SELECT
    event_id,
    event_time,
    customer_id,
    merchant_id,
    amount,
    currency,
    country,
    device_id,
    merchant_category,
    channel,
    topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    validated_at,
    validation_batch_id
FROM {{ source('pulseguard', 'transactions') }}
WHERE event_id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY
        validated_at DESC,
        kafka_offset DESC
) = 1