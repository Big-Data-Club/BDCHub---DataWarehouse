-- Shared evidence envelope. Payload is allowlisted by a versioned source contract.
CREATE TABLE IF NOT EXISTS `${project_id}.bdc_raw_${environment}.platform_events` (
  event_id STRING NOT NULL,
  source_name STRING NOT NULL,
  schema_version INT64 NOT NULL,
  record_source STRING NOT NULL,
  source_event_id STRING NOT NULL,
  occurred_at TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL,
  user_id INT64,
  course_id INT64,
  request_id STRING,
  session_id STRING,
  capture_method STRING NOT NULL,
  capture_id STRING,
  kafka_topic STRING,
  kafka_partition INT64,
  kafka_offset INT64,
  payload_sha256 STRING NOT NULL,
  payload JSON NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source_name, course_id, user_id, event_id
OPTIONS (
  require_partition_filter = TRUE,
  partition_expiration_days = 60,
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'Versioned allowlisted source evidence; real events only; metadata and identifiers; no content or secrets',
  labels = [('application','bdc'),('layer','raw'),('mode','sandbox')]
);
