-- Runtime release DDL: datasets must already exist.
-- Dataset provisioning remains an operator-only action in 001_foundation.sql.
-- One row is one delivery of a real LMS micro-interaction from Kafka.
-- No arbitrary metadata/content is admitted into this table.
CREATE TABLE IF NOT EXISTS `${project_id}.bdc_raw_${environment}.lms_micro_interactions` (
  event_id STRING NOT NULL OPTIONS(description = 'Namespaced original interaction_id'),
  interaction_id INT64 NOT NULL,
  user_id INT64 NOT NULL,
  course_id INT64 NOT NULL,
  lesson_id INT64,
  node_id INT64,
  action_type STRING NOT NULL,
  score FLOAT64,
  status STRING,
  created_at TIMESTAMP NOT NULL OPTIONS(description = 'Original LMS event time'),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description = 'Actual warehouse ingestion time'),
  kafka_topic STRING NOT NULL,
  kafka_partition INT64 NOT NULL,
  kafka_offset INT64 NOT NULL,
  payload_sha256 STRING NOT NULL OPTIONS(description = 'Hash of the canonical allowlisted source payload')
)
PARTITION BY DATE(ingested_at)
CLUSTER BY course_id, user_id, action_type
OPTIONS (
  require_partition_filter = TRUE,
  partition_expiration_days = 60,
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'Real lms.analytics.interactions deliveries; at least once; no synthetic records',
  labels = [('application', 'bdc'), ('source', 'lms'), ('mode', 'sandbox')]
);
