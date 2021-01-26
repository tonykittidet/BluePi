CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.USER_LOG_DATA` AS
SELECT 
  created_at,
  updated_at,
  id,
  user_id,
  action,
  CAST(status as Boolean) as success
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.user_log`
