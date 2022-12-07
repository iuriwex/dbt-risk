--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates ACR dataset that is used for reporting and in collections analytics visualizations
--Dependant on:
--  Repo:DP_CollectionAnalytics\Scheduled_Scripts\
--      Script: ACR_ Daily count_PROD.sql
--Schedule: Daily, 0800
--Run Time: 5-10 minutes
---Dependent on acr_daily_callcount_new-----------------
with pro_sandbox.ca_acr_dataset as (
    SELECT * FROM {{('stg_eltool__ca_acR_dataset')}}
)
with agentcount as (
    select 
        task_create_date,
        primary_skill,
        lob,
        count(distinct agent_name) agent_count
    from pro_sandbox.ca_acr_daily_callcount
    where primary_skill is not null
    group by task_create_date, lob, primary_skill
),
casescount as (
    select 
        date_driver,
        var_q,
        lob,
        count(distinct casenumber) nbr_open_cases
    from pro_sandbox.ca_collections_analytics_audit_data_model
    group by date_driver, var_q, lob
)
SELECT
    a.date_driver,
    a.var_q,
    a.lob,
    a.nbr_open_cases,
    b.agent_count
from casescount a 
left join agentcount b
on a.date_driver=b.task_create_date
and a.var_q=b.primary_skill
and a.lob=b.lob
