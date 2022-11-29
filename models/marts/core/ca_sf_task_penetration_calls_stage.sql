--creates dataset of collection calls that were made
with pro_sandbox.ca_sf_task_penetration_calls_stage as (
    SELECT * FROM {{('stg_elool__ca_sf_penetration_calls_stage')}}
)
 select
 s.case_id
,s.case_casenumber
,st.cxone_contact_id__c
,call_attempts_needed
,nbr_of_agent_calls
,max(st.id) as taskid
,count(*) as Nbr_of_Calls
,null:datetime as Create_Date  -- new field added, based on the DML alter table
from
 (select  
   case_id
  ,case_casenumber
  ,q_type
  ,taskid 
  ,max(call_attempts_needed) as call_attempts_needed
  ,sum(case when task_owner_agent_name NOT IN ('Credit Monitoring','CreditApplication Site Guest User','NICE Integration') then 1
          else 0 end) as nbr_of_agent_calls
  ,count(*) as nbr_of_case_records
  from pro_sandbox.ca_salesforce1
  group by case_id, case_casenumber, q_type,taskid) s
left outer join
salesforce_rss.sf_task st
 on s.case_id = st.whatid
AND s.taskid = st.id
left outer join salesforce_rss.sf_user         u 
        on st.ownerid = u.id
WHERE
coalesce(st.ae_activity_type__c,st.activity_type__c) like '%Call%'
and st.subject NOT IN ('Dialer Task - ISDN Cause Code 21','Dialer Task - Error','Dialer Task - ISDN Cause Code 18'
  ,'Dialer Task - ISDN Cause Code 102','Dialer Task - Manual Suppression')
and ((st.cxone_contact_id__c IS NOT NULL) AND (st.cxone_contact_id__c <>0))
and case when nbr_of_agent_calls >0 then 'Y' else 'N' end =
    case when u.name IN ('Credit Monitoring','CreditApplication Site Guest User','NICE Integration') then 'N'
         else 'Y' end
 GROUP BY
 s.case_id, s.case_casenumber,st.cxone_contact_id__c,call_attempts_needed,nbr_of_agent_calls
UNION ALL
 select
 s.case_id
,s.case_casenumber
,st.cxone_contact_id__c
,call_attempts_needed
,nbr_of_agent_calls
,st.id
,1 as Nbr_of_Calls
from
 (select  
   case_id
  ,col_id
  ,case_casenumber
  ,q_type
  ,taskid  
  ,max(call_attempts_needed) as call_attempts_needed
  ,sum(case when task_owner_agent_name NOT IN ('Credit Monitoring','CreditApplication Site Guest User','NICE Integration') then 1
          else 0 end) as nbr_of_agent_calls
  ,count(*) as nbr_of_case_records
  from pro_sandbox.ca_salesforce
  group by case_id, col_id, case_casenumber, q_type,taskid) s
left outer join
salesforce_rss.sf_task st
 on s.col_id = st.whatid
AND s.taskid = st.id
left outer join salesforce_rss.sf_user         u 
        on st.ownerid = u.id
WHERE
coalesce(st.ae_activity_type__c,st.activity_type__c) like '%Call%'
and st.subject NOT IN ('Dialer Task - ISDN Cause Code 21','Dialer Task - Error','Dialer Task - ISDN Cause Code 18'
  ,'Dialer Task - ISDN Cause Code 102','Dialer Task - Manual Suppression')
and ((st.cxone_contact_id__c IS NOT NULL) AND  (st.cxone_contact_id__c <> '0'))
and case when nbr_of_agent_calls >0 then 'Y' else 'N' end =
    case when u.name IN ('Credit Monitoring','CreditApplication Site Guest User','NICE Integration') then 'N'
         else 'Y' end;

-- authoir Iuri <iuri.dearaujo@wexinc.com         
--adds create date field to dataset above
-- alter table pro_sandbox.ca_sf_task_penetration_calls_stage1
   -- add Create_Date datetime null;