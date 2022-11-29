--Creates a dataset that is used to identify the population that needs to be
--pulled for aging purposes. 
with pro_sandbox.ca_salesforce_driver as (
    SELECT * FROM {{('stg_eltool__ca_salesforce_driver')}}
)
select a.case_casenumber
      ,a.match_key
      ,a.case_create_dt
      ,a.task_create_dt      
      ,max(a.case_closed_dt) as case_closed_dt
      ,max(a.lob) as lob 
  from pro_sandbox.ca_salesforce_stage1 a 
 group by a.case_casenumber, a.match_key, a.case_create_dt, a.task_create_dt;