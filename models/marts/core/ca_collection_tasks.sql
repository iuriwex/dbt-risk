/*  Nick Bogan, 2021-04-28: I reverted to loading ca_collection_tasks driving from
 * a subquery against pro_sandbox.ca_collection_cases that returns the case and
 * collection IDs, because we need all current or former Collections-owned cases
 * as built in ca_driver and loaded in ca_collection_cases.
 */

--Creates dataset of unique collection tasks that need to be appended for final dataset
with  pro_sandbox.ca_collection_tasks as (
    SELECT * FROM {{('stg_eltool__ca_collection_tasks')}}
)
select
 t.id                             taskid
,t.whatid                         taskwhatid
,t.subject                        task_subject
,t.type                           task_type
,case when t.type='Email' then 1 else 0 end as email_contacted
,case when t.type='Call'  then 1 else 0 end as call_contacted   
,convert_timezone('UTC','EST',t.created_time__c::timestamp)        as task_create_dttm
,trunc(convert_timezone('UTC','EST',t.created_time__c::timestamp)) as task_create_dt   
,trunc(convert_timezone('UTC','EST',t.created_time__c::timestamp))  as task_or_create_dt
,t.activity_type__c               task_activity_type
,t.ae_activity_type__c            task_ae_activity_type
,t.calldisposition                task_calldisposition
,t.disposition_name__c            task_disposition_name
,t.check_in_reason__c             task_check_in_reason
,t.call_attempts__c               task_call_attempts
,t.cxone_contact_id__c            task_cxone_contact_id
,case when t.calldisposition = 'Connect' then 1 else 0 end                               as task_calldisposition_connect
,case when t.calldisposition = 'No Connect' then 1 else 0 end                            as task_calldisposition_no_connect
,case when t.calldisposition = 'Right Party Contact' then 1 else 0 end                   as task_calldisposition_rpc
,case when t.calldisposition = 'Right Party Contact w/ Promise to Pay' then 1 else 0 end as task_calldisposition_rpc_ptp
,u.name                           task_owner_agent_name
from -- case_col
    (select DISTINCT case_id as id from pro_sandbox.ca_collection_cases1
    union all
    --  My understanding is that SFDC IDs are globally distinct, and there was no
    -- overlap between these IDs in entapps_stage on 2021-04-21.
    select DISTINCT col_id as id from pro_sandbox.ca_collection_cases1) case_col
inner join salesforce_rss.sf_task t
    on t.whatid = case_col.id
left outer join salesforce_rss.sf_contact      c 
    on c.id = t.whoid
left outer join salesforce_rss.sf_user         u 
    on t.ownerid = u.id
