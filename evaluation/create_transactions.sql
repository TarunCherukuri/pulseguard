CREATE TABLE IF NOT EXISTS pulseguard.transactions (
  event_id STRING NOT NULL,
  event_time TIMESTAMP,
  customer_id STRING,
  merchant_id STRING,
  amount FLOAT64,
  currency STRING,
  country STRING,
  device_id STRING,
  merchant_category STRING,
  channel STRING,
  topic STRING,
  kafka_partition INT64,
  kafka_offset INT64,
  kafka_timestamp TIMESTAMP,
  validated_at TIMESTAMP,
  validation_batch_id INT64
)
PARTITION BY DATE(event_time)
CLUSTER BY customer_id, merchant_id;
