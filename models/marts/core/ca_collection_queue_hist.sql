with ca_collection_queue_hist as (
    SELECT * FROM {{ ('stg_eltool__ca_collection_queue_hist')}}
)
select
b.casenumber,
b.owner_name__c as current_caseowner_name,
nvl(a.caseid,b.id) as id,
a.createddate as fromdate_ts_utc, ------createddate is pulled as is inorder for the dense rank to function properly by taking the orginal timestamp into consideration
NULLIF(b.closeddate,' ') as caseclosed_ts_utc,----pulled in for validation, is not part of the final query
cast(NULLIF(b.closeddate,' ') as date) as caseclosed_dt_utc,----pulled in for validation, is not part of the final query
convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp) as fromdate_ts,
nvl(
    case when convert_timezone('UTC','EST',a.createddate) = lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate) 
         then lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate)
         else lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate)- interval '1 seconds' end,
case 
    when a.createddate > left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)
        then convert_timezone('UTC','EST',a.createddate)
    when NULLIF(b.closeddate,' ') IS NOT NULL
        then convert_timezone('UTC','EST',left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)::timestamp)
    when cast(NULLIF(b.closeddate,' ') as date)=cast(a.createddate as date)--- This is to satisfy the usecase where the case jumps multiple queue within the same day and closes on the same day as it was created. This logic ensures that, under such circumstances the to_date is same as the created date and not one day before so that these cases don't get omitted from the dataset.
        then convert_timezone('UTC','EST',a.createddate)
    else '9999-12-31' end) todate_ts,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp)) as fromdate,----Fromdate will act as pivot to make appropriate date joins with other tables
cast(nvl(lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate) - interval '1 seconds',
    case when NULLIF(b.closeddate,' ') < a.createddate
            then convert_timezone('UTC','EST',a.createddate)
         when NULLIF(b.closeddate,' ') IS NOT NULL
            then convert_timezone('UTC','EST',left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)::timestamp)
         when cast(NULLIF(b.closeddate,' ') as date)=cast(a.createddate as date)--- This is to satisfy the usecase where the case jumps multiple queue within the same day and closes on the same day as it was created. This logic ensures that, under such circumstances the to_date is same as the created date and not one day before so that these cases don't get omitted from the dataset.
            then convert_timezone('UTC','EST',a.createddate) 
         else '9999-12-31' end) as date) todate,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
row_number() OVER(PARTITION BY a.caseid, cast(a.createddate as date) order by a.caseid, a.createddate desc) as IntraDay_Row_Number,
case
    when a.newvalue like 'Collections%' then a.newvalue
    else a.oldvalue end as value,
case 
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%NAF%' 
        then 'NAF'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%OTR%' 
        then 'OTR' end as lob,
case
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%HVAR%'
        then 'HVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%MVAR%'
        then 'MVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%LVAR%'
        then 'LVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Self-cure%'
        then 'SELFCURE'
when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Strategic%'
        then 'STRATEGIC ACCOUNTS'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Late Stage%'
        then 'LATE STAGE'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Outsourced%'
        then 'OUTSOURCED'
    else 'OTHER' end as var_q,
dense_rank() over(partition by b.casenumber
            order by a.createddate) as row_order
from
salesforce_rss.sf_case_history a
    left outer join salesforce_dl_rss.case b 
        on a.caseid = b.id
where
a.field = 'Owner'---Looks up the created date and other meta data only when the field is owner so that we are capturing the information only when then there is a queue migration.
and trunc(a.createddate)>= '2020-01-01'
and (a.oldvalue = 'Credit Monitoring'
or substring(a.oldvalue, 1, 10)= 'Collection'
or a.newvalue = 'Credit Monitoring'
or substring(a.newvalue, 1, 10)= 'Collection');