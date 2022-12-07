--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates a Queue Migration dataset that will be used for collection analysis and visualization
--Dependant on:
--  Repo:DP_CollectionAnalytics\Scheduled_Scripts\
--      Script: Create_Collection_Analytics_Dataset_PROD.sql
--Schedule: Daily, 0800
--Run Time: 5-10 minutes
---Dependent on Collections Analytics master dataset------------------------------------
with pro_sandbox.ca_queue_migration as (
    with fulldata as (
        select distinct
            b.casenumber,
            a.createdbyid,
            e.name
            --       a.caseid         as casehistory_caseid
            --      ,b.id             as case_id
            ,a.createddate
            ,trunc(a.createddate) as created_dt
            ,case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end       as value
            ,case when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%NAF%' then 'NAF'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%OTR%' then 'OTR'
            end as lob
            ,case when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%HVAR%'       then 'HVAR'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%MVAR%'       then 'MVAR'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%LVAR%'       then 'LVAR'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Self-cure%'  then 'SELFCURE'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Returned Payment%'  then 'RETURN PAYMENT'
                    when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Outsourced%'  then 'OUTSOURCED'
                else 'OTHER(LATE STAGE,STRATEGIC ACCOUNTS, DEFAULT, SOLD ACCOUNTS)' end as var_q       
            ,dense_rank() over(partition by b.casenumber order by a.createddate)   as row_order      
        from salesforce_rss.sf_case_history a 
        left outer join salesforce_dl_rss.case         b
        on a.caseid = b.id    
        left outer join salesforce_dl_rss.account      c
        on b.accountid = c.id       
        left outer join salesforce_rss.sf_contract__c  d
        on c.id = d.account__c
        left outer join salesforce_rss.sf_user e 
        on a.createdbyid=e.id
        where b.casenumber in (
            select distinct case_casenumber from pro_sandbox.ca_collections_analytics
        ) 
        and a.field='Owner'
        and trunc(a.createddate)>= '2020-01-01'
        and (
            a.oldvalue='Credit Monitoring' or substring(a.oldvalue,1,10)='Collection'
            or a.newvalue='Credit Monitoring' or substring(a.newvalue,1,10)='Collection'
        )
    )
    -- and b.casenumber='04896848')
    ,migration as (
        select a.casenumber
            ,b.createddate
            ,a.created_dt                      as from_queue_date
            ,nvl(b.created_dt,'9999-12-31')    as to_queue_date
            ,a.row_order,
            nvl(a.lob,b.lob) lob,
            a.var_q  as from_q
            ,b.var_q  as to_q,
            b.name
        from fulldata    a
        left outer join fulldata    b
        on a.casenumber = b.casenumber 
        and a.row_order = b.row_order - 1
        where nvl(b.created_dt,'9999-12-31') != '9999-12-31'  
        order by a.casenumber, a.createddate
    ) 
    select *
    from migration  a
    where a.from_q != a.to_q
    and a.from_queue_date <= a.to_queue_date
    order by a.casenumber, a.from_queue_date);
--End 4.DP-CollectionsAnalytics\Individual_Scripts\Queue Migration_PROD.sql
--Start 5.DP-CollectionsAnalytics\Individual_Scripts\Cure Rates_PROD.sql

