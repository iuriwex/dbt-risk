-- Creates a dataset to pull the max collection history date
-- attached to a task so that the var score from mckinsey
-- can be correctly pulled into final dataset.
with pro_sandbox.ca_ColHistoryTaskDt as (
    SELECT * 
    FROM {{('stg_eltool__ca_ColHistoryTaskDt')}}
)
SELECT
  s.taskid,
  s.task_create_dttm,
  hcol.id as hcolid,
  max(hcol.row_created_ts) as max_utc_hcol_createdttm,
  convert_timezone('UTC','EST',left(regexp_replace(max(hcol.row_created_ts),'T',' '),19)::timestamp) max_et_hcol_createdttm,
  count(*) as nbr_of_records
FROM pro_sandbox.ca_salesforce s
LEFT OUTER JOIN salesforce_rss.history_sf_collections hcol
ON s.col_id = hcol.id
AND s.task_create_dttm > convert_timezone('UTC','EST',left(regexp_replace(hcol.row_created_ts,'T',' '),19)::timestamp)
GROUP BY s.taskid, s.task_create_dttm, hcol.id