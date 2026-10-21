-- Sandbox-compatible transformations: views, without INSERT/UPDATE/MERGE.
-- Discover retained data by ingestion time, including old event-time arrivals.
CREATE OR REPLACE VIEW `${project_id}.bdc_staging_${environment}.lms_micro_interactions`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'One row per original LMS interaction; conflicting identities excluded'
)
AS
WITH deliveries AS (
  SELECT *,
    COUNT(DISTINCT payload_sha256) OVER (PARTITION BY event_id) AS payload_versions,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY ingested_at, kafka_topic, kafka_partition, kafka_offset
    ) AS delivery_rank
  FROM `${project_id}.bdc_raw_${environment}.lms_micro_interactions`
  WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT * EXCEPT (payload_versions, delivery_rank)
FROM deliveries
WHERE payload_versions = 1 AND delivery_rank = 1;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.fact_content_interaction`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'Grain: one real LMS micro-interaction; source keys preserved; no fabricated dimensions'
)
AS
SELECT
  event_id, interaction_id, user_id, course_id, lesson_id, node_id,
  action_type, score, status,
  created_at AS occurred_at,
  DATE(created_at, 'Asia/Ho_Chi_Minh') AS activity_date,
  ingested_at,
  kafka_topic, kafka_partition, kafka_offset,
  'lms-service' AS record_source
FROM `${project_id}.bdc_staging_${environment}.lms_micro_interactions`;

CREATE OR REPLACE VIEW `${project_id}.bdc_mart_${environment}.mart_course_daily_activity`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'Course x Vietnam reporting date x action_type; interaction counts and distinct actors, not whole-platform DAU'
)
AS
SELECT
  activity_date,
  course_id,
  action_type,
  COUNT(*) AS interaction_count,
  COUNT(DISTINCT user_id) AS active_learners,
  MAX(ingested_at) AS latest_ingested_at
FROM `${project_id}.bdc_core_${environment}.fact_content_interaction`
GROUP BY activity_date, course_id, action_type;

CREATE OR REPLACE VIEW `${project_id}.bdc_governance_${environment}.lms_identity_conflicts`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
  description = 'Same original event identity with conflicting source payloads; not admitted to facts'
)
AS
SELECT
  event_id,
  COUNT(*) AS delivery_count,
  COUNT(DISTINCT payload_sha256) AS payload_versions,
  MIN(ingested_at) AS first_ingested_at,
  MAX(ingested_at) AS latest_ingested_at
FROM `${project_id}.bdc_raw_${environment}.lms_micro_interactions`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
  AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY event_id
HAVING COUNT(DISTINCT payload_sha256) > 1;
