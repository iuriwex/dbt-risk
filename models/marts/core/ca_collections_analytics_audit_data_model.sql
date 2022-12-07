--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: The objective of the new data model is to provide day to day historical information and activities about a case. 
--         It can effectively track the queue migrations, thus providing a complete history of a case on daily level.
--Dependant on:
--  Repo:DP_CollectionAnalytics\Scheduled_Scripts\
--      Script: Create_Collection_Analytics_Dataset_PROD.sql
--Schedule: Daily, 0800
--Run Time: 5-10 minutes
---Dependent on Collections Analytics master dataset------------------------------------
with pro_sandbox.ca_collections_analytics_audit_data_model as (
    SELECT * FROM {{('stg_eltool__ca_collections_analytics_audit_data_model')}}
)
----Driver CTE gets all possible date values by using task and case created date)
with driver as (
    select
    distinct cast(DATE_TRUNC('day',a.task_create_dttm) as date) date_driver 
    from pro_sandbox.ca_collections_analytics a 
    union 
    select 
    distinct  cast(DATE_TRUNC('day',b.case_created_dttm) as date) date_driver 
    from pro_sandbox.ca_collections_analytics b 
),
-----qtype cte tracks queue migration for every case found in dev_final12. It appends a new row for every queue change.
qtype as (
select
b.casenumber,
b.owner_name__c as current_caseowner_name,
 nvl(b.id, a.caseid) as id,
 a.createddate as fromdate_ts_utc, ------createddate is pulled as is inorder for the dense rank to function properly by taking the orginal timestamp into consideration
 b.closeddate,----pulled in for validation, is not part of the final query
 cast(b.closeddate as date),----pulled in for validation, is not part of the final query
convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp) as fromdate_ts,
 trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp)) as fromdate,----Fromdate will act as pivot to make appropriate date joins with other tables
    nvl(cast(lead(a.createddate,1) over (partition by casenumber order by a.createddate)as date)-1 ,
    case when b.closeddate > ' '
         then trunc(convert_timezone('UTC','EST',left(regexp_replace(b.closeddate,'T',' '),19)::timestamp))
         when cast(NULLIF(b.closeddate,' ') as date)=cast(a.createddate as date) --- This is to satisfy the usecase where the case jumps multiple queue within the same day and closes on the same day as it was created. This logic ensures that, under such circumstances the to_date is same as the created date and not one day before so that these cases don't get omitted from the dataset.
         then cast(a.createddate as date)
         else '9999-12-31' end) todate,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
    case
        when a.newvalue like 'Collections%' then a.newvalue
        else a.oldvalue end as value
      ,case when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%NAF%' then 'NAF'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%OTR%' then 'OTR'
        end as lob
      ,case when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%HVAR%'       then 'HVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%MVAR%'       then 'MVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%LVAR%'       then 'LVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Self-cure%'  then 'SELFCURE'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Returned Payment%'  then 'RETURN PAYMENT'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Outsourced%'  then 'OUTSOURCED'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Late Stage%'  then 'LATE STAGE'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Strategic Accounts%'  then 'STRATEGIC ACCOUNTS'
        else 'OTHER(DEFAULT, SOLD ACCOUNTS,FOLLOW UP)' end as var_q,
                dense_rank() over(partition by b.casenumber
            order by a.createddate) as row_order
            from
                salesforce_rss.sf_case_history a
            left outer join salesforce_dl_rss.case b on
                a.caseid = b.id
            where
            --or (a.oldvalue like 'Collections%' or a.newvalue like 'Collections%')
             --casenumber='04506830' and
                b.casenumber in (select distinct case_casenumber from pro_sandbox.ca_collections_analytics) -- to show the case migration history for all the cases in dev_final12 dataset
                and
                a.field = 'Owner'---Looks up the created date and other meta data only when the field is owner so that we are capturing the information only when then there is a queue migration.
                and trunc(a.createddate)>= '2020-01-01'
                and (a.oldvalue = 'Credit Monitoring'
                or substring(a.oldvalue, 1, 10)= 'Collection'
                or a.newvalue = 'Credit Monitoring'
                or substring(a.newvalue, 1, 10)= 'Collection'))
select 
a.date_driver,
b.casenumber,
b.current_caseowner_name,
b.id,
b.fromdate,
 b.todate,
b.var_q,
b.lob,
c.task_create_dttm,
c.task_ae_activity_type,
c.task_activity_type,
c.task_owner_agent_name,
c.task_cxone_contact_id,
max(e.q_type)most_recent_queue,---pulling in the recent queue as a couple of views that project case level info require it. Using the var_q coming from the qtype cte would result in duplication for view dealing with case counts, $collected,$presented as they can change multiple queues overtime.
c.penetration_calls_made,---used to identify legit calls
c.task_disposition_name,
c.task_calldisposition,
d.case_secondary_reason,
d.case_brought_current,
max(f.created_date)created_date,
max(e.amtpastdue_amt)amtpastdue_amt-- pulling it to support one of timeline view viz pertaining to case and $collected, $presented.
from 
driver a 
left join 
qtype b
on 
a.date_driver >= b.fromdate and a.date_driver <=b.todate ------ This join ensures that we are capturing the historic information on a daily basis for every case.
left join 
pro_sandbox.ca_collections_analytics c
on 
b.casenumber=c.case_casenumber
and 
cast(DATE_TRUNC('day',task_create_dttm) as date)=date_driver
left join 
(select case_casenumber,
case when case_closed_dttm is not null 
         then cast(case_closed_dttm as date)
         else '9999-12-31' end as case_closed_date,
         case_secondary_reason,case_brought_current
from pro_sandbox.ca_collections_analytics
group by case_casenumber,case_created_dttm,
case when case_closed_dttm is not null then 
          cast(case_closed_dttm as date)
         else '9999-12-31' end,
         case_secondary_reason,case_brought_current) d 
on 
b.casenumber=d.case_casenumber
and 
a.date_driver=d.case_closed_date
left join 
(select
case_casenumber,
max(amtpastdue_amt)amtpastdue_amt,
q_type
from pro_sandbox.ca_collections_analytics
group by 
case_casenumber,
q_type) e 
on 
b.casenumber=e.case_casenumber
left join 
(select casenumber,min(fromdate)created_date from qtype group by casenumber)f
on 
b.casenumber=f.casenumber
and
a.date_driver=f.created_date
--where 
--fromdate='2020-07-08' and 
--casenumber is not null
group by 
a.date_driver,
b.casenumber,
b.current_caseowner_name,
b.id,
b.var_q,
 b.fromdate,
 b.todate,
b.lob,
c.task_create_dttm,
c.task_ae_activity_type,
c.task_activity_type,
c.task_owner_agent_name,
c.task_cxone_contact_id,
c.penetration_calls_made,
c.task_disposition_name,
c.task_calldisposition,
d.case_secondary_reason,
d.case_brought_current
--End 6.DP-CollectionsAnalytics\Individual_Scripts\Collections- New Data Model_PROD.sql
--Start 7.DP-COllectionsAnalytics\Individual_Scripts\ACR_ Daily count_PROD.sql

