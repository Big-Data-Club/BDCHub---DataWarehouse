-- Raw -> staging -> conformed stars. No new data marts in this foundation phase.
CREATE OR REPLACE VIEW `${project_id}.bdc_staging_${environment}.platform_events`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
WITH deliveries AS (
  SELECT *,
    COUNT(DISTINCT payload_sha256) OVER (PARTITION BY event_id) AS versions,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at, capture_id, kafka_partition, kafka_offset) AS delivery_rank
  FROM `${project_id}.bdc_raw_${environment}.platform_events`
  WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT * EXCEPT(versions,delivery_rank) FROM deliveries
WHERE versions=1 AND delivery_rank=1;

CREATE OR REPLACE VIEW `${project_id}.bdc_governance_${environment}.platform_identity_conflicts`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
SELECT event_id, source_name, COUNT(*) AS deliveries, COUNT(DISTINCT payload_sha256) AS payload_versions,
       MIN(ingested_at) AS first_ingested_at, MAX(ingested_at) AS latest_ingested_at
FROM `${project_id}.bdc_raw_${environment}.platform_events`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
  AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY event_id,source_name
HAVING COUNT(DISTINCT payload_sha256)>1;

CREATE OR REPLACE VIEW `${project_id}.bdc_governance_${environment}.learning_identity_conflicts`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
WITH deliveries AS (
 SELECT event_id,payload_sha256,ingested_at
 FROM `${project_id}.bdc_raw_${environment}.lms_micro_interactions`
 WHERE ingested_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 60 DAY)
   AND ingested_at<TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 1 DAY)
 UNION ALL
 SELECT event_id,payload_sha256,ingested_at
 FROM `${project_id}.bdc_raw_${environment}.platform_events`
 WHERE ingested_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 60 DAY)
   AND ingested_at<TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 1 DAY)
   AND source_name='lms.interaction'
)
SELECT event_id,COUNT(*) AS deliveries,COUNT(DISTINCT payload_sha256) AS payload_versions,
       MIN(ingested_at) AS first_ingested_at,MAX(ingested_at) AS latest_ingested_at
FROM deliveries GROUP BY event_id HAVING COUNT(DISTINCT payload_sha256)>1;

-- Reconcile the previously deployed LMS landing table with the new envelope.
-- Identical deliveries collapse; cross-path conflicts do not enter the new fact.
CREATE OR REPLACE VIEW `${project_id}.bdc_staging_${environment}.learning_events`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
WITH combined AS (
  SELECT event_id, interaction_id, user_id, course_id, lesson_id, node_id,
         action_type, score, status, created_at AS occurred_at, ingested_at,
         payload_sha256, kafka_topic, kafka_partition, kafka_offset
  FROM `${project_id}.bdc_raw_${environment}.lms_micro_interactions`
  WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  UNION ALL
  SELECT event_id, CAST(JSON_VALUE(payload,'$$.interaction_id') AS INT64), user_id, course_id,
         CAST(JSON_VALUE(payload,'$$.lesson_id') AS INT64), CAST(JSON_VALUE(payload,'$$.node_id') AS INT64),
         JSON_VALUE(payload,'$$.action_type'), CAST(JSON_VALUE(payload,'$$.score') AS FLOAT64),
         JSON_VALUE(payload,'$$.status'), occurred_at, ingested_at, payload_sha256,
         kafka_topic,kafka_partition,kafka_offset
  FROM `${project_id}.bdc_raw_${environment}.platform_events`
  WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
    AND ingested_at < TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    AND source_name='lms.interaction'
), ranked AS (
  SELECT *, COUNT(DISTINCT payload_sha256) OVER (PARTITION BY event_id) AS versions,
         ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at,kafka_partition,kafka_offset) AS delivery_rank
  FROM combined
)
SELECT * EXCEPT(versions,delivery_rank) FROM ranked WHERE versions=1 AND delivery_rank=1;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.fact_learning_activity`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
 description='Grain: one original LMS micro-interaction; observed actors, not global logins') AS
SELECT event_id, interaction_id,
       TO_HEX(SHA256(CONCAT('bdc:user:',CAST(user_id AS STRING)))) AS user_sk,
       TO_HEX(SHA256(CONCAT('bdc:course:',CAST(course_id AS STRING)))) AS course_sk,
       TO_HEX(SHA256(CONCAT('lms:',action_type))) AS feature_sk,
       CAST(FORMAT_DATE('%Y%m%d',DATE(occurred_at,'Asia/Ho_Chi_Minh')) AS INT64) AS date_sk,
       EXTRACT(HOUR FROM DATETIME(occurred_at,'Asia/Ho_Chi_Minh'))*60+
         EXTRACT(MINUTE FROM DATETIME(occurred_at,'Asia/Ho_Chi_Minh')) AS time_sk,
       user_id,course_id,lesson_id,node_id,action_type,score,status,
       occurred_at,DATE(occurred_at,'Asia/Ho_Chi_Minh') AS activity_date,
       ingested_at,payload_sha256,kafka_topic,kafka_partition,kafka_offset,
       1 AS event_count,'lms-service' AS record_source
FROM `${project_id}.bdc_staging_${environment}.learning_events`;

-- Preserve the already-deployed fact/mart contract during the landing cutover.
-- This compatibility projection adds no new mart or business measure.
CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.fact_content_interaction`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
 description='Compatibility projection of reconciled learning activity; original columns preserved') AS
SELECT event_id,interaction_id,user_id,course_id,lesson_id,node_id,action_type,score,status,
       occurred_at,activity_date,ingested_at,kafka_topic,kafka_partition,kafka_offset,record_source
FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.fact_recommendation_event`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
 description='Grain: one recommendation event; impressions and outcomes remain separate rows') AS
SELECT event_id, source_event_id,
       TO_HEX(SHA256(CONCAT('bdc:user:',CAST(user_id AS STRING)))) AS user_sk,
       TO_HEX(SHA256(CONCAT('bdc:course:',CAST(course_id AS STRING)))) AS course_sk,
       CAST(FORMAT_DATE('%Y%m%d',DATE(occurred_at,'Asia/Ho_Chi_Minh')) AS INT64) AS date_sk,
       user_id,course_id,occurred_at,ingested_at,
       JSON_VALUE(payload,'$$.event_type') AS event_type,
       JSON_VALUE(payload,'$$.recommendation_id') AS recommendation_id,
       JSON_VALUE(payload,'$$.recommendation_set_id') AS recommendation_set_id,
       JSON_VALUE(payload,'$$.surface') AS surface,
       JSON_VALUE(payload,'$$.entity_type') AS entity_type,
       JSON_VALUE(payload,'$$.entity_id') AS entity_id,
       CAST(JSON_VALUE(payload,'$$.rank') AS INT64) AS rank,
       1 AS event_count,payload_sha256
FROM `${project_id}.bdc_staging_${environment}.platform_events`
WHERE source_name='recommendation.event';

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.fact_llm_attempt`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
 description='Grain: one recorded provider attempt; unknown usage/cost remain null') AS
SELECT event_id, CAST(source_event_id AS INT64) AS usage_id,
       CASE WHEN JSON_VALUE(payload,'$$.provider_code') IS NOT NULL OR JSON_VALUE(payload,'$$.model_name') IS NOT NULL
         THEN TO_HEX(SHA256(TO_JSON_STRING(STRUCT(JSON_VALUE(payload,'$$.provider_code') AS provider,
              JSON_VALUE(payload,'$$.model_name') AS model)))) END AS model_sk,
       CAST(FORMAT_DATE('%Y%m%d',DATE(occurred_at,'Asia/Ho_Chi_Minh')) AS INT64) AS date_sk,
       occurred_at,ingested_at,request_id,user_id,course_id,
       JSON_VALUE(payload,'$$.task_code') AS task_code,
       JSON_VALUE(payload,'$$.provider_code') AS provider_code,
       JSON_VALUE(payload,'$$.model_name') AS model_name,
       CAST(JSON_VALUE(payload,'$$.success') AS BOOL) AS success,
       CAST(JSON_VALUE(payload,'$$.fallback_used') AS BOOL) AS fallback_used,
       CAST(JSON_VALUE(payload,'$$.attempt_no') AS INT64) AS attempt_no,
       CAST(JSON_VALUE(payload,'$$.latency_ms') AS INT64) AS latency_ms,
       CAST(JSON_VALUE(payload,'$$.prompt_tokens') AS INT64) AS prompt_tokens,
       CAST(JSON_VALUE(payload,'$$.completion_tokens') AS INT64) AS completion_tokens,
       CAST(JSON_VALUE(payload,'$$.prompt_tokens') AS INT64)+CAST(JSON_VALUE(payload,'$$.completion_tokens') AS INT64) AS total_tokens,
       JSON_VALUE(payload,'$$.usage_status') AS usage_status,
       JSON_VALUE(payload,'$$.error_class') AS error_class,
       CAST(NULL AS NUMERIC) AS cost_amount,CAST(NULL AS STRING) AS currency,
       'unavailable_no_historical_price' AS cost_status,
       'unavailable_in_historical_source' AS actor_attribution,
       1 AS attempt_count,payload_sha256
FROM `${project_id}.bdc_staging_${environment}.platform_events`
WHERE source_name='ai.llm_attempt';

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_user`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY),
 description='Inferred observed members; no fabricated identity, roles or SCD history') AS
WITH observed AS (
 SELECT user_sk,user_id,occurred_at FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`
 UNION ALL SELECT user_sk,user_id,occurred_at FROM `${project_id}.bdc_core_${environment}.fact_recommendation_event`
)
SELECT user_sk,user_id,MIN(occurred_at) AS first_observed_at,MAX(occurred_at) AS last_observed_at,
       CAST(NULL AS STRING) AS display_name,CAST(NULL AS STRING) AS role,
       'inferred_member' AS member_status,'not_available' AS attribute_history
FROM observed WHERE user_id IS NOT NULL GROUP BY user_sk,user_id;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_course`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
WITH observed AS (
 SELECT course_sk,course_id,occurred_at FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`
 UNION ALL SELECT course_sk,course_id,occurred_at FROM `${project_id}.bdc_core_${environment}.fact_recommendation_event`
)
SELECT course_sk,course_id,MIN(occurred_at) AS first_observed_at,MAX(occurred_at) AS last_observed_at,
       CAST(NULL AS STRING) AS title,CAST(NULL AS STRING) AS publication_state,
       'inferred_member' AS member_status,'not_available' AS attribute_history
FROM observed WHERE course_id IS NOT NULL GROUP BY course_sk,course_id;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_feature`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
SELECT DISTINCT feature_sk,CONCAT('lms:',action_type) AS feature_code,action_type,
       'lms-service' AS owning_service,'observed_source_action' AS taxonomy_status
FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_date`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
WITH dates AS (
 SELECT DATE(occurred_at,'Asia/Ho_Chi_Minh') AS full_date FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`
 UNION DISTINCT SELECT DATE(occurred_at,'Asia/Ho_Chi_Minh') FROM `${project_id}.bdc_core_${environment}.fact_recommendation_event`
 UNION DISTINCT SELECT DATE(occurred_at,'Asia/Ho_Chi_Minh') FROM `${project_id}.bdc_core_${environment}.fact_llm_attempt`
)
SELECT CAST(FORMAT_DATE('%Y%m%d',full_date) AS INT64) AS date_sk,full_date,
       EXTRACT(YEAR FROM full_date) AS calendar_year,EXTRACT(QUARTER FROM full_date) AS calendar_quarter,
       EXTRACT(MONTH FROM full_date) AS calendar_month,EXTRACT(ISOYEAR FROM full_date) AS iso_year,
       EXTRACT(ISOWEEK FROM full_date) AS iso_week,EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
       EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) AS is_weekend,'Asia/Ho_Chi_Minh' AS reporting_timezone
FROM dates;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_time`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
SELECT DISTINCT time_sk,DIV(time_sk,60) AS hour_of_day,MOD(time_sk,60) AS minute_of_hour,
       'Asia/Ho_Chi_Minh' AS reporting_timezone
FROM `${project_id}.bdc_core_${environment}.fact_learning_activity`;

CREATE OR REPLACE VIEW `${project_id}.bdc_core_${environment}.dim_ai_model`
OPTIONS (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)) AS
SELECT model_sk,provider_code,model_name,MIN(occurred_at) AS first_observed_at,
       MAX(occurred_at) AS last_observed_at,'observed_model_identity_only' AS member_status
FROM `${project_id}.bdc_core_${environment}.fact_llm_attempt`
WHERE model_sk IS NOT NULL GROUP BY model_sk,provider_code,model_name;
