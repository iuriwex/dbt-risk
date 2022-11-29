--Creates a distinct list  of calls to tie back to final dataset
with pro_sandbox.ca_sf_task_penetration_calls1 as (
    SELECT * FROM {{('stg_eltool__ca_cf_task_penetration_calls')}}
)
select distinct 
case_id
,case_casenumber
,create_Date
,cxone_contact_id__c
,call_attempts_needed
,taskid
,Nbr_of_Calls
from 
pro_sandbox.ca_sf_task_penetration_calls_stage;
