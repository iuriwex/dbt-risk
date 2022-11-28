with pro_sandbox.ca_TaskLevelActionFlagC as (
    SELECT * 
    FROM {{('ca_taskLevelActionFlagC')}}
)
  select
  cht.taskid,cht.task_create_dttm,cht.hcolid,cht.max_utc_hcol_createdttm,cht.max_et_hcol_createdttm,
  cht.nbr_of_records,cf.action_flag__c as collections_action_flag
  from
  pro_sandbox.ca_ColHistoryTaskDt1 cht
inner join salesforce_rss.history_sf_collections cf
        on cht.hcolid = cf.id
       AND cht.max_utc_hcol_createdttm = cf.createddate;