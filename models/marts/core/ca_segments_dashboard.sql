--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates Segment Analysis Dataset that will be used for collection analysis and visualization
--Dependant on:
--  Repo:DP_CollectionAnalytics\Scheduled_Scripts\
--      Script: Create_Collection_Analytics_Dataset_PROD.sql
--Schedule: Daily, 0800
--Run Time: 5-10 minutes
---Dependent on Collections Analytics master dataset------------------------------------
with pro_sandbox.ca_segments_dashboard as (
    SELECT * FROM {{('stg_eltool__ca_segments_dashboard')}}
)
with dollar_presented as (
    select 
        q_type,
        lob,
        naf_setid,
        otr_platform_sub,
        case_created_dttm::date,
        count(case_casenumber)total_cases,
        sum(dollar_presented)dollar_presented
    from (
        select 
            case_casenumber,
            case_created_dttm::date,
            q_type,
            lob,
            naf_setid,
            otr_platform_sub,
            max(amtpastdue_amt)dollar_presented
        from pro_sandbox.ca_collections_analytics1
        --where setid='REV' and q_type='MVAR' and lob='NAF'
        group by case_casenumber, case_created_dttm::date, q_type, lob, naf_setid, otr_platform_sub
    )
    group by q_type, case_created_dttm::date, lob, naf_setid, otr_platform_sub
), dollar_collected as (
    select 
        q_type,
        lob,
        naf_setid,
        otr_platform_sub,
        case_created_dttm::date,
        count(case_casenumber)total_closed,
        sum(dollar_collected)dollar_collected
    from (
        select 
            case_casenumber,
            case_created_dttm::date,
            q_type,
            lob,
            naf_setid,
            otr_platform_sub,
            max(amtpastdue_amt) dollar_collected
        from pro_sandbox.ca_collections_analytics1 
        where case_secondary_reason='Customer Current' and case_status='Closed'--and setid='REV' and q_type='MVAR' and lob='NAF'
        group by case_casenumber, case_created_dttm::date, q_type, lob, naf_setid, otr_platform_sub
    )
    group by q_type,  lob, naf_setid, otr_platform_sub, case_created_dttm::date
),
select 
    a.*,
    b.total_closed,
    b.dollar_collected
from dollar_presented a 
left join dollar_collected b
on a.q_type=b.q_type  and a.lob=b.lob and a.naf_setid=b.naf_setid and a.case_created_dttm::date=b.case_created_dttm::date
where a.lob='NAF'
union all
select 
    a.*,
    b.total_closed,
    b.dollar_collected
from dollar_presented a 
left join dollar_collected b
on a.q_type=b.q_type and a.lob=b.lob and a.case_created_dttm::date=b.case_created_dttm::date and a.otr_platform_sub=b.otr_platform_sub
where a.lob in ('OTR')
