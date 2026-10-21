-- Real BDC project. Billing remains disabled; no data rows are generated here.
-- Dataset/table/view expiration follows the BigQuery Sandbox limit.
CREATE SCHEMA IF NOT EXISTS `${project_id}.bdc_raw_${environment}`
OPTIONS (
  location = '${location}',
  description = 'BDC approved source events; platform/data owner; billing disabled',
  default_table_expiration_days = 60,
  default_partition_expiration_days = 60,
  labels = [('application', 'bdc'), ('layer', 'raw'), ('mode', 'sandbox')]
);
CREATE SCHEMA IF NOT EXISTS `${project_id}.bdc_staging_${environment}`
OPTIONS (
  location = '${location}',
  description = 'BDC normalization and deduplication views',
  default_table_expiration_days = 60,
  labels = [('application', 'bdc'), ('layer', 'staging'), ('mode', 'sandbox')]
);
CREATE SCHEMA IF NOT EXISTS `${project_id}.bdc_core_${environment}`
OPTIONS (
  location = '${location}',
  description = 'BDC conformed business facts; live source data only',
  default_table_expiration_days = 60,
  labels = [('application', 'bdc'), ('layer', 'core'), ('mode', 'sandbox')]
);
CREATE SCHEMA IF NOT EXISTS `${project_id}.bdc_mart_${environment}`
OPTIONS (
  location = '${location}',
  description = 'BDC reporting aggregates; reporting timezone Asia/Ho_Chi_Minh',
  default_table_expiration_days = 60,
  labels = [('application', 'bdc'), ('layer', 'mart'), ('mode', 'sandbox')]
);
CREATE SCHEMA IF NOT EXISTS `${project_id}.bdc_governance_${environment}`
OPTIONS (
  location = '${location}',
  description = 'BDC data quality and ingestion observability',
  default_table_expiration_days = 60,
  labels = [('application', 'bdc'), ('layer', 'governance'), ('mode', 'sandbox')]
);

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
