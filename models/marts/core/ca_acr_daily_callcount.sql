--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates ACR Dataset that will be used for collection analysis, reporting and visualization
--Dependant on:
--  Repo:DP_CollectionAnalytics\Scheduled_Scripts\
--      Script: Collections- New Data Model_PROD.sql
--Schedule: Daily, 0800
--Run Time: 5-10 minutes
--Dependent on Collections- New Data Model------------------------
with pro_sandbox.ca_acr_daily_callcount as (
    SELECT * FROM {{('stg_eltool__ca_acr_daily_callcount')}}
)
with agent_call_callcount as (
select
a.task_owner_agent_name agent_name,
var_q,
lob,
count(distinct a.task_cxone_contact_id) call_count,
DATE_TRUNC('day',task_create_dttm)::date task_create_date,
DENSE_RANK() over (PARTITION BY a.task_owner_agent_name,DATE_TRUNC('day',task_create_dttm)::date order by count(distinct a.task_cxone_contact_id) desc) rank_order,
case when 
DENSE_RANK() over (PARTITION BY a.task_owner_agent_name,DATE_TRUNC('day',task_create_dttm)::date order by count(distinct a.task_cxone_contact_id) desc)=1
then min(var_q)
end primary_skill
from pro_sandbox.ca_collections_analytics_audit_data_model a 
--where DATE_TRUNC('day',task_create_dttm)::date='2020-10-01'
--where a.task_owner_agent_name='Valerie Harris Russell' and DATE_TRUNC('day',task_create_dttm)::date='2020-10-01'
group by 
a.task_owner_agent_name,
var_q,
lob,
DATE_TRUNC('day',task_create_dttm)::date),
primary_skill_assignment as (
select 
agent_name,
task_create_date,
lob,
min(primary_skill)primary_skill
from 
agent_call_callcount
where agent_name not in ('James Harrell','NICE Integration')
group by 
agent_name,
lob,
task_create_date)
select * from primary_skill_assignment
--End 7.DP-COllectionsAnalytics\Individual_Scripts\ACR_ Daily count_PROD.sql
--Start 8.DP-COllectionsAnalytics\Individual_Scripts\ACR_ dataset_PROD.sql
