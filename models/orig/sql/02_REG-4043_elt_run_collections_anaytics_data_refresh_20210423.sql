/*  Nick Bogan, 2021-04-23: When we build ca_driver, we don't bother doing a full
 * outer join of case and sf_case_history, because ca_driver is inner joined to
 * case when loading ca_collection_cases.
 */

--Creates a dataset of all cases that were ever assigned to collections 
--to be used as a base population for the entirety of the collections
--data model/set creation
drop table if exists pro_sandbox.ca_driver1;
create table pro_sandbox.ca_driver1 as
select a.id,
    max(case 
        when b.newvalue like 'Collections%' then b.newvalue 
        when b.oldvalue like 'Collections%' then b.oldvalue
        else a.owner_name__c end) as owner_name__c 
from salesforce_dl_rss.case            a
     left outer join
     salesforce_rss.sf_case_history    b
   on a.id = b.caseid
where (a.owner_name__c like 'Collections%' or b.oldvalue like 'Collections%' or b.newvalue like 'Collections%')
group by a.id;


--Creates a dataset of case & collection object attributes
drop table if exists pro_sandbox.ca_collection_cases1;
create table pro_sandbox.ca_collection_cases1 AS
select DISTINCT
    c.casenumber case_casenumber
    ,case when length(a.wex_account__c)>2 then a.wex_account__c else col.ar_number__c end as match_key   
    ,c.account_status__c              case_account_status
    ,c.age_hours__c                   case_age_hours
    ,c.carrier_id__c                  case_carrier_id   
    ,c.owner_name__c                  case_owner_name__c
    ,convert_timezone('UTC','EST',left(regexp_replace(c.createddate,'T',' '),19)::timestamp)               as case_created_dttm
    ,trunc(convert_timezone('UTC','EST',left(regexp_replace(c.createddate,'T',' '),19)::timestamp))        as case_create_dt   
    ,case when c.closeddate > ' ' then convert_timezone('UTC','EST',left(regexp_replace(c.closeddate,'T',' '),19)::timestamp) end  as case_closed_dttm
    ,case when c.closeddate > ' ' then trunc(convert_timezone('UTC','EST',left(regexp_replace(c.closeddate,'T',' '),19)::timestamp))
          else '9999-12-31' end  as case_closed_dt         
    ,c.credit_limit__c                case_credit_limit
    ,driver.owner_name__c             driver_owner_name
    ,c.status                         case_status
    ,c.primary_reason__c 
    ,c.secondary_reason__c            case_secondary_reason
    ,case when c.secondary_reason__c = 'Customer Current' then 'TRUE' else 'FALSE' end   as case_brought_current
    ,c.rollup_past_due_amount__c                case_rollup_past_due_amount
    ,col.past_due_amount__c         col_past_due_amt
    ,col.id                  col_id
    ,c.id                             case_id
    ,c.dialer_status__c               case_dialer_status
    ,a.acct_row_id__c                 naf_cust_id
    ,a.wex_account__c                 wex_account
    ,col.recourse_code__c as Recourse_Code
    ,col.collector_code__c as Collector_Code
    ,a.account_since__c as Account_Since
    ,col.suspense_class__c as Suspense_Class
    ,col.language_indicator__c as Language_Indicator
    ,col.statement_cycle__c as Statement_Cycle
    ,col.payment_method__c as PaymentMethod
    ,case when driver.owner_name__c like '%HVAR%'       then 'HVAR'
          when driver.owner_name__c like '%MVAR%'       then 'MVAR'
          when driver.owner_name__c like '%LVAR%'       then 'LVAR'
          when driver.owner_name__c like '%Self-cure%'  then 'SELFCURE'
          when driver.owner_name__c like '%Strategic%'  then 'STRATEGIC ACCOUNTS'
          when driver.owner_name__c like '%Outsourced%' then 'OUTSOURCED'  
          when driver.owner_name__c like '%Late Stage%' then 'LATE STAGE'         
          else 'OTHER' end as q_type
    ,a.platform__c
    ,case when a.platform_lob__c='N/A' and driver.owner_name__c like '%Agenda and Leaders Guide%' then driver.owner_name__c
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%Skill Based Training%'     then driver.owner_name__c
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%Factoring%'                then driver.owner_name__c     
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%NAF%'                      then 'NAF' 
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%OTR%'                      then 'OTR'     
          else a.platform_lob__c 
         end as lob
    ,col.ar_number__c                 contract_ar_number               
    ,col.statement_cycle__c         col_statement_cycle 
    ,case when driver.owner_name__c LIKE ('%LVAR%') then 1
          when driver.owner_name__c LIKE ('%MVAR%') then 1
          when driver.owner_name__c LIKE ('%HVAR%') then 1
          else -1 end  as call_attempts_needed
    ,convert_timezone('UTC','EST',cast(NULLIF(c.follow_up_date__c,'') as datetime)) as Follow_Up_Date_Current
    ,a.national_account_otr__c as national_account_otr
    ,a.credit_national_account__c as credit_national_account
    ,a.pfs_rep__c as pfs_rep
    ,a.direct_debit__c as direct_debit
    ,0 as task_level
FROM pro_sandbox.ca_driver AS driver
INNER JOIN salesforce_dl_rss.case c
ON c.id = driver.id
INNER JOIN salesforce_dl_rss.account a
ON c.accountid  = a.id 
left outer join salesforce_rss.sf_collections col
    on c.id        = col.case__c 
   AND c.accountid = col.account__c;
  
  
  
  
--Flags the record that is most appropriate to attach a task too
--as without attaching a task to a unique record will cause massive
--duplication of data.    
update pro_sandbox.ca_collection_cases1
set task_level = 1 
from 
pro_sandbox.ca_collection_cases1 cccd
    inner join (select case_casenumber, case_id, min(col_id) as mincolid
    from pro_sandbox.ca_collection_cases1
    group by case_casenumber,case_id)  coltask
        on cccd.col_id = coltask.mincolid
      AND cccd.case_id = coltask.case_id;

     
     
     
/*  Nick Bogan, 2021-04-28: I reverted to loading ca_collection_tasks driving from
 * a subquery against pro_sandbox.ca_collection_cases that returns the case and
 * collection IDs, because we need all current or former Collections-owned cases
 * as built in ca_driver and loaded in ca_collection_cases.
 */

--Creates dataset of unique collection tasks that need to be appended for final dataset
drop table if exists pro_sandbox.ca_collection_tasks1;
create table pro_sandbox.ca_collection_tasks1 as
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
    on t.ownerid = u.id;




/*  Nick Bogan, 2021-04-23: The load of ca_salesforce_stagesort joined ca_collection_cases
 * and ca_collection_tasks using OR, which was very slow. We replace that with the
 * case_task subquery, a UNION ALL of three queries against the two tables (inner
 * join the first way, inner join the second [mutually exclusive] way, antijoin
 * both ways) with no logic in the joins, which is much faster.
 */

--Creates a dataset that joins cases and tasks together
drop table if exists pro_sandbox.ca_salesforce_stagesort1;
create table pro_sandbox.ca_salesforce_stagesort1 as 
select case_task.*,
    case when task_calldisposition in
            ('Promise to Pay',
            'Right Party Contact - Promise to Pay',
            'Right Party Contact - With Promise to Pay',
            'Right Party Contact w/ Promise to Pay',
            'Right Party Contact with pmt',
            'ptp')
         then 1 else 0 end as calldisp_ptpflg,
    0 as ptp_level
from -- case_task
    (select 'Case Task' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    inner join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.case_id = cctd.taskwhatid
       AND cccd.task_level = 1
    union all
    select 'Collection Task' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    inner join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.col_id = cctd.taskwhatid
       AND cccd.task_level = 1
    union all
    select 'Other' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    left outer join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.case_id = cctd.taskwhatid
       AND cccd.task_level = 1
    left outer join pro_sandbox.ca_collection_tasks1 cctd_col
        on cccd.col_id = cctd_col.taskwhatid
       AND cccd.task_level = 1
    where cctd.taskwhatid is null
        and cctd_col.taskwhatid is null
    ) case_task;
   
   
drop table if exists pro_sandbox.ca_salesforce_stage1;
create table pro_sandbox.ca_salesforce_stage1 as
select 
row_number() OVER (order by case_casenumber, calldisp_ptpflg, task_create_dttm) as uniquerowid,
*
from 
pro_sandbox.ca_salesforce_stagesort1
order by uniquerowid;


--Drops staging table 
drop table if exists pro_sandbox.ca_salesforce_stagesort1;

--Flags the appropriate level to attach a promise to pay record
update pro_sandbox.ca_salesforce_stage1
set ptp_level = 1
from 
pro_sandbox.ca_salesforce_stage1 cssd
    inner join
    (select case_id, col_id,
        max(case when calldisp_ptpflg = 1 then UniqueRowID else NULL end) as MaxUniqueRowID1,
        max(uniquerowid) as maxUniqueRowID
    from pro_sandbox.ca_salesforce_stage1
    group by case_id, col_id) b 
        on cssd.UniqueRowID = coalesce(b.MaxUniqueRowID1,b.MaxUniqueRowID);


       
--Creates dataset that introduces promise to pay elements
drop table if exists pro_sandbox.ca_salesforce1;
create table pro_sandbox.ca_salesforce1 as
SELECT 
 cssd.*
,p.ptpid
,p.ptp_create_dt
,p.first_payment_date__c          ptp_first_payment_dt
,coalesce(p.payment_plan_total__c,p.payment_amount__c)  ptp_payment_amt
,p.payment_frequency__c           ptp_payment_freq
,p.payment_type__c                ptp_payment_type
,sup.name                         ptp_agent
,pp.days_since_promised_for__c    ptp_days_since_promised
,pp.payment_amount__c             pymtpln_payment_amount
,pp.payment_date__c               pymtpln_payment_date
,pp.payment_remitted__c           pymtpln_payment_remitted
,supp.name                        pymtpln_agent
FROM pro_sandbox.ca_salesforce_stage cssd
LEFT OUTER JOIN   
             (select collections__c,first_payment_date__c,payment_plan_total__c,payment_amount__c,payment_frequency__c,payment_type__c
             ,id as ptpid,cast(createddate as date) as ptp_create_dt,createdbyid,
             dense_rank() over(partition by collections__c order by row_created_ts desc) as first_payment_date_rankdesc          
             from salesforce_rss.sf_promise_to_pay
             )  p
        on cssd.col_id = p.collections__c
       AND p.first_payment_date_rankdesc = 1 
       AND cssd.ptp_level = 1 
    left outer join salesforce_rss.sf_payment_plan pp 
        on cssd.col_id = pp.collections__c
    left outer join salesforce_rss.sf_user sup
        on p.createdbyid = sup.id
    left outer join salesforce_rss.sf_user supp
        on pp.createdbyid  = supp.id;


--Creates a dataset that is used to identify the population that needs to be
--pulled for aging purposes. 
drop table if exists pro_sandbox.ca_salesforce_driver1;
create table pro_sandbox.ca_salesforce_driver1 as
select a.case_casenumber
      ,a.match_key
      ,a.case_create_dt
      ,a.task_create_dt      
      ,max(a.case_closed_dt) as case_closed_dt
      ,max(a.lob) as lob 
  from pro_sandbox.ca_salesforce_stage1 a 
 group by a.case_casenumber, a.match_key, a.case_create_dt, a.task_create_dt;




/*  Nick Bogan, 2021-04-23: We create PRO_sandbox tables for the NAF and OTR past
 * due data, for better performance joining to ca_salesforce_driver. Also, we combine
 * the former ca_get_amtpastdue, ca_get_rev_amtpastdue and ca_get_rev_amtmindue
 * tables into one ca_get_amt_due table, to reduce joins when building
 * ca_collections_analytics. When we do so, we remove the measure fields from
 * these queries' GROUP BY clauses. For example, we don't group by
 * nvl(b.past_due_amount, c.past_due_total) in the first subquery. I don't see
 * why we would want separate rows for each measure amount in these data.
 */

drop table if exists pro_sandbox.ca_pastdue_naf1;
create table pro_sandbox.ca_pastdue_naf1 as
select pd.cust_id,
    pd.business_date,
    pd.past_due_amount
from collections_history_prod_rss.nafleet_past_due as pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.nafleet_past_due as pd2);

drop table if exists pro_sandbox.ca_pastdue_otr1;
create table pro_sandbox.ca_pastdue_otr1 as
select pd.ar_number,
    pd.ar_date,
    pd.past_due_total
from collections_history_prod_rss.otr_past_due pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.otr_past_due as pd2);
   
   
   
   
   
   
   
   
drop table if exists pro_sandbox.ca_get_amt_due1;
create table pro_sandbox.ca_get_amt_due1 as
select case_casenumber,
    case_create_dt,
    max(begin_amtpastdue_dt)        as begin_amtpastdue_dt,
    max(end_amtpastdue_dt)          as end_amtpastdue_dt,
    sum(amtpastdue_amt)             as amtpastdue_amt,
    max(begin_rev_amtpastdue_dt)    as begin_rev_amtpastdue_dt,
    max(end_rev_amtpastdue_dt)      as end_rev_amtpastdue_dt,
    sum(rev_amtpastdue_amt)         as rev_amtpastdue_amt,
    max(begin_rev_billing_dt)       as begin_rev_billing_dt,
    max(end_rev_billing_dt)         as end_rev_billing_dt,
    sum(rev_minamtdue_amt)          as rev_minamtdue_amt
from -- amt_due
(select
--  Past due amounts from psfin (non revolver)
    a.case_casenumber,
    a.case_create_dt,
    min(case when a.lob='NAF' then b.business_date when a.lob='OTR' then c.ar_date end)  as begin_amtpastdue_dt,
    max(case when a.lob='NAF' then b.business_date when a.lob='OTR' then c.ar_date end)  as end_amtpastdue_dt,    
    max(nvl(b.past_due_amount, c.past_due_total))                                        as amtpastdue_amt,
    cast(null as date) as begin_rev_amtpastdue_dt,
    cast(null as date) as end_rev_amtpastdue_dt,
    0 as rev_amtpastdue_amt,
    cast(null as date) as begin_rev_billing_dt,
    cast(null as date) as end_rev_billing_dt,
    0 as rev_minamtdue_amt
from pro_sandbox.ca_salesforce_driver1           a 
     left outer join pro_sandbox.ca_pastdue_naf1 b
  on a.match_key   = b.cust_id
 and b.business_date between a.case_create_dt and a.case_closed_dt
     left outer join pro_sandbox.ca_pastdue_otr c
  on a.match_key   = c.ar_number
 and c.ar_date between a.case_create_dt and a.case_closed_dt 
group by a.case_casenumber,
    a.case_create_dt
union all
select
--  Past due amounts (revolver)
    a.case_casenumber,
    a.case_create_dt,
    cast(null as date) as begin_amtpastdue_dt,
    cast(null as date) as end_amtpastdue_dt,
    0 as amtpastdue_amt,
    min(b.business_date)   as begin_rev_amtpastdue_dt,
    max(b.business_date)   as end_rev_amtpastdue_dt, 
    max(b.wx_age30+b.wx_age60+b.wx_age90+b.wx_age120+b.wx_age150+b.wx_age180) as rev_amtpastdue_amt,
    cast(null as date) as begin_rev_billing_dt,
    cast(null as date) as end_rev_billing_dt,
    0 as rev_minamtdue_amt
 from pro_sandbox.ca_salesforce_driver1                  a
      left outer join
      naflt_psfin_rss.ps_wx_cust_daily   b
   on a.match_key = b.cust_id
  and b.business_unit='REV' 
  and b.business_date between a.case_create_dt and a.case_closed_dt
where (b.wx_age30+b.wx_age60+b.wx_age90+b.wx_age120+b.wx_age150+b.wx_age180) > 0  
 group by a.case_casenumber,
    a.case_create_dt
union all
select
-- Revolver aging
    a.case_casenumber,
    a.case_create_dt,
    cast(null as date) as begin_amtpastdue_dt,
    cast(null as date) as end_amtpastdue_dt,
    0 as amtpastdue_amt,
    cast(null as date) as begin_rev_amtpastdue_dt,
    cast(null as date) as end_rev_amtpastdue_dt,
    0 as rev_amtpastdue_amt,
    min(b.wxf_billing_date)  as begin_rev_billing_dt,
    max(b.wxf_billing_date)  as end_rev_billing_dt,   
    max(b.wx_minimum_due)    as rev_minamtdue_amt
 from pro_sandbox.ca_salesforce_driver1                  a
      left outer join
      naflt_psfin_rss.ps_wx_rev_hdr_stg  b
   on a.match_key = b.wx_ext_cust_id
  and b.wxf_billing_date between a.case_create_dt and a.case_closed_dt
where b.wx_minimum_due > 0 
 group by a.case_casenumber,
    a.case_create_dt
) amt_due
group by case_casenumber,
    case_create_dt;
   
   
   
--Creates dataset of all the payments that have come in for OTR and NAF
drop table if exists pro_sandbox.ca_payments1;
create table pro_sandbox.ca_payments1 as
select cust.cust_id          as match_key
      ,pay.post_dt           as payment_dt
      ,sum(pay.payment_amt)  as payment_amt
from naflt_psfin_rss.ps_payment         pay
    ,naflt_psfin_rss.ps_payment_id_cust cust
where pay.deposit_bu = cust.deposit_bu
  and pay.deposit_id = cust.deposit_id
  and cust.payment_seq_num = pay.payment_seq_num
  and cust.id_seq_num = cust.id_seq_num
  and pay.post_dt >= '2020-01-01'
group by cust.cust_id, pay.post_dt
UNION ALL
select c.ar_number           as match_key  
      ,a.payment_date        as payment_dt     
      ,sum(a.payment_amount) as payment_amt
from efs_owner_crd_rss.efs_payments a
     inner join
     (select c.ar_number
            ,c.creditline_id
       from efs_owner_crd_rss.efs_ar_master  c
       group by c.ar_number, c.creditline_id
     ) c
  on a.creditline_id = c.creditline_id
 and a.payment_date >= '2020-01-01'    
group by c.ar_number, a.payment_date;





drop table if exists pro_sandbox.ca_sf_payments1;
create table pro_sandbox.ca_sf_payments1 as
select a.match_key
      ,a.case_casenumber
      ,a.case_create_dt
      ,min(b.payment_dt)  as begin_payment_dt
      ,max(b.payment_dt)  as end_payment_dt
      ,max(b.payment_amt) as payment_amt
 from pro_sandbox.ca_salesforce_driver   a
      inner join
      pro_sandbox.ca_payments            b
   on a.match_key = b.match_key
  and b.payment_dt between a.case_create_dt and a.case_closed_dt
 group by a.match_key, a.case_casenumber, a.case_create_dt; 



--Creates a dataset of unique promise to pay entries that need to be matched
drop table if exists pro_sandbox.ca_ptp_unique1;
create table pro_sandbox.ca_ptp_unique1 as 
select DISTINCT
       ptpid
      ,ptp_create_dt
      ,match_key
      ,ptp_first_payment_dt
      ,ptp_payment_amt
      ,ptp_payment_freq
      ,ptp_payment_type
 from pro_sandbox.ca_salesforce;



--Creates dataset the unique promise to pay entries to payments
--that will be used in final output 
drop table if exists pro_sandbox.ca_promise_payments1;
create table pro_sandbox.ca_promise_payments1 as
select a.match_key
      ,a.ptpid
      ,a.ptp_create_dt
      ,cast(a.ptp_first_payment_dt as date) as ptp_first_payment_date
      ,sum(b.payment_amt)                   as ttl_paid_to_promise
      ,count(b.match_key)                   as nbr_of_pymts_to_promise 
from pro_sandbox.ca_ptp_unique a
inner join 
pro_sandbox.ca_payments   b
on a.match_key = b.match_key
and b.payment_dt BETWEEN a.ptp_create_dt AND cast(a.ptp_first_payment_dt as date)
group by a.match_key, a.ptpid, a.ptp_create_dt, cast(a.ptp_first_payment_dt as date);






--Creates a dataset to assign OTR accounts to Fleet One or EFSLLC
drop table if exists pro_sandbox.ca_otr_platform1;
create table pro_sandbox.ca_otr_platform1 as 
select DISTINCT 
ar_number,
platform,
issuer_name
from efs_owner_crd_rss.efs_ar_master eam ;






--Creates a nice acd dataset to use for final dataset
drop table if exists pro_sandbox.ca_nice_acd1;
create table pro_sandbox.ca_nice_acd1 as 
select distinct 
          cast(a.contact_id as varchar) as nice_contact_id 
        ,NULLIF(a.acw__time,'')acw__time
        ,NULLIF(a.abandon__time,'')abandon__time
        ,NULLIF(a.active__talk__time,'')active__talk__time
        ,NULLIF(a.agent_id,'')agent_id
        ,NULLIF(a.agent__name,'')agent__name
        ,NULLIF(a.available__time,'')available__time
        ,NULLIF(a.callback__time,'')callback__time
        ,NULLIF(a.concurrent__time,'')concurrent__time
        ,NULLIF(a.consult__time,'')consult__time
        ,NULLIF(a.contact__duration,'')contact__duration
        ,NULLIF(a.contact__start__date__time,'')contact__start__date__time
        ,NULLIF(a.contact__end__date__time,'')contact__end__date__time
        ,NULLIF(a.contact__time,'')contact__time
        ,NULLIF(a.contact__type,'')contact__type
        ,NULLIF(a.direction,'')direction
        ,NULLIF(a.handle__time,'')handle__time
        ,NULLIF(a.hold__time,'')hold__time
        ,NULLIF(a.ivr__time,'')ivr__time
        ,NULLIF(a.inbound_aht,'')inbound_aht
        ,NULLIF(a.inqueue__time,'')inqueue__time
        ,NULLIF(a.outbound_aht,'')outbound_aht
        ,NULLIF(a.parked__time,'')parked__time
        ,NULLIF(a.refused__time,'')refused__time
        ,NULLIF(a.speed__of__answer,'')speed__of__answer
        ,NULLIF(a.team__name,'')team__name
        ,NULLIF(a.unavailable__time,'')unavailable__time
        ,NULLIF(a.wait__time,'')wait__time
        ,NULLIF(a.working__time,'')working__time
from nice_acd_rss.wex__aht_reportdownload   a
where a.active__agent='True';
















--creates dataset of collection calls that were made
drop table if exists pro_sandbox.ca_sf_task_penetration_calls_stage1;
create table pro_sandbox.ca_sf_task_penetration_calls_stage1 as 
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
        
        







--Pulls max create date for the taskid. This had to be culled out from the main query because the max task id for the
--day does mean that that task id will have the max create date due to the way salesforce reserves primary keys 
--during user entry
update pro_sandbox.ca_sf_task_penetration_calls_stage1
    set create_date = (select max(b.task_create_dttm) from pro_sandbox.ca_salesforce b where a.taskid = b.taskid)
FROM pro_sandbox.ca_sf_task_penetration_calls_stage1 a;
    
--Creates a distinct list  of calls to tie back to final dataset
drop table if exists pro_sandbox.ca_sf_task_penetration_calls1;
create table pro_sandbox.ca_sf_task_penetration_calls1 as 
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


--Creates a dataset to pull the max collection history date
--attached to a task so that the var score from mckinsey
--can be correctly pulled into final dataset.
drop table if exists pro_sandbox.ca_ColHistoryTaskDt1;
create table pro_sandbox.ca_ColHistoryTaskDt1 as
SELECT
  s.taskid,
  s.task_create_dttm,
  hcol.id as hcolid,
  max(hcol.row_created_ts) as max_utc_hcol_createdttm,
  convert_timezone('UTC','EST',left(regexp_replace(max(hcol.row_created_ts),'T',' '),19)::timestamp) max_et_hcol_createdttm,
  count(*) as nbr_of_records
FROM pro_sandbox.ca_salesforce s
LEFT OUTER JOIN salesforce_rss.history_sf_collections hcol
ON s.col_id = hcol.id
AND s.task_create_dttm > convert_timezone('UTC','EST',left(regexp_replace(hcol.row_created_ts,'T',' '),19)::timestamp)
GROUP BY s.taskid, s.task_create_dttm, hcol.id;






drop table if exists pro_sandbox.ca_TaskLevelActionFlagC1;
create table pro_sandbox.ca_TaskLevelActionFlagC1 as 
select
  cht.taskid,cht.task_create_dttm,cht.hcolid,cht.max_utc_hcol_createdttm,cht.max_et_hcol_createdttm,
  cht.nbr_of_records,cf.action_flag__c as collections_action_flag
from pro_sandbox.ca_ColHistoryTaskDt1 cht
inner join salesforce_rss.history_sf_collections cf
on cht.hcolid = cf.id
AND cht.max_utc_hcol_createdttm = cf.createddate;



--Creates a staging table of the var information for a specific collections id
--to be used for the next part of the process.
drop table if exists pro_sandbox.ca_VARHistory1;
create table pro_sandbox.ca_VARHistory1 as 
select
a.case_id,a.match_key,'NAF' as lob_table_source,max(cast(mnaf.partition_0 as date)) as MaxScoringDt,count(DISTINCT mnaf.business_date) as NbrOfScoringDays
from
pro_sandbox.ca_salesforce a
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mnaf
        on a.match_key = mnaf.cust_id 
where     
cast(mnaf.partition_0 as date) BETWEEN cast(a.case_created_dttm as date) AND cast(coalesce(a.case_closed_dttm,getdate()) as date) 
group by
a.case_id,a.match_key
UNION ALL 
select
a.case_id,a.match_key,'OTR' ,max(cast(motr.partition_0 as date)),count(DISTINCT motr.business_date) 
from
pro_sandbox.ca_salesforce a
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_OTR motr
        on a.match_key = motr.cust_id 
where     
cast(motr.partition_0 as date) BETWEEN cast(a.case_created_dttm as date) AND cast(coalesce(a.case_closed_dttm,getdate()) as date) 
group by
a.case_id,a.match_key;







--Creates a dataset of the VAR Info by case for the output
--of the process.
drop table if exists pro_sandbox.ca_VarInfobyCase1;
create table pro_sandbox.ca_VarInfobyCase1 as
select 
vh.case_id,
vh.match_key,
vh.lob_table_source,
coalesce(cast(mnaf.partition_0 as date),cast(motr.partition_0 as date)) as scoringdt,
coalesce(mnaf.wx_days_past_due,motr.wx_days_past_due) as wx_days_past_due,
coalesce(mnaf.dpd_bucket,motr.dpd_bucket) as dpd_bucket,
coalesce(mnaf.p_1,motr.p_1) as var_rate__c,
coalesce(cast(NULLIF(mnaf.va_r,'') as decimal(18,2)),cast(NULLIF(motr.va_r,'') as decimal(18,2))) as var__c,
coalesce(mnaf.reasoncode1,motr.reasoncode1) as reasoncode1,
coalesce(mnaf.reasoncode2,motr.reasoncode2) as reasoncode2,
coalesce(mnaf.reasoncode3,motr.reasoncode3) as reasoncode3
from
pro_sandbox.ca_VARHistory vh
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mnaf
        on vh.match_key = mnaf.cust_id 
       AND vh.MaxScoringDt = cast(mnaf.partition_0 as date) 
       AND vh.lob_table_source = 'NAF'
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_OTR motr
        on vh.match_key = motr.cust_id 
           AND vh.MaxScoringDt = cast(motr.partition_0 as date)
           AND vh.lob_table_source = 'OTR';





/*  Nick Bogan, 2021-04-23: We build ca_efs_customer_current to simplify building
 * ca_collections_analytics. It's a *LOT* faster to use a max run date subquery
 * to get the run date than to use a window function over the whole table to find
 * the max run date. 
 */

drop table if exists pro_sandbox.ca_efs_customer_current1;
create table pro_sandbox.ca_efs_customer_current1 as
select cast(customer_id as varchar) as cust_id,
--  customer_ID isn't unique in EFS_customer--platform is also required--but I see
-- no good platform to join on in the Salesforce data, so we pick the alphabetical
-- max platform to avoid duplication.
    max(platform) as platform
from efs_owner_crd_rss.efs_customer
where run_date =
    (select max(c.run_date) as max_run_date
    from efs_owner_crd_rss.efs_customer c)
group by customer_id;




--Creates final output that weaves a lot of the above tables together
--as Tableau cannot handle joins with data this large 
drop table if exists pro_sandbox.ca_collections_analytics1;
create table pro_sandbox.ca_collections_analytics1 as
select distinct 
       a.*
      ,case when g.setid='REV' then b.begin_rev_amtpastdue_dt else b.begin_amtpastdue_dt end as begin_amtpastdue_dt
      ,case when g.setid='REV' then b.end_rev_amtpastdue_dt   else b.end_amtpastdue_dt end   as end_amtpastdue_dt
      ,coalesce(case when g.setid='REV' then b.rev_amtpastdue_amt      
                     else b.amtpastdue_amt end,cast(NULLIF(a.case_rollup_past_due_amount,'') as decimal(18,2)))      as amtpastdue_amt 
      ,b.begin_rev_billing_dt
      ,b.end_rev_billing_dt
      ,b.rev_minamtdue_amt
      ,case when a.ptp_payment_amt <= e.ttl_paid_to_promise then TRUE else FALSE end as promise_kept
      ,e.ttl_paid_to_promise
      ,e.nbr_of_pymts_to_promise      
      ,f.begin_payment_dt
      ,f.end_payment_dt
      ,f.payment_amt      
      ,case when a.lob='OTR' then i.platform 
            when a.lob='NAF' then g.setid 
            else null end as business_unit    
      ,g.name1
      ,h.l1_acct_nm
      ,g.setid    as naf_setid      
      ,i.platform as otr_platform
      ,j.*
      ,k.call_attempts_needed as penetration_call_needed
      /* updating the logic behind penetration_calls_made to capture the genesys despotioned calls. The genesys despoitioned calls don't have a task_cxone_contact_id and the task_owner_agent_name 
      for all the Genesys despositioned calls is "Customer Service Operations"  */
      ,case when a.case_id = k.case_id AND a.taskid = k.taskid AND (a.task_owner_agent_name not in ('Customer Service Operations') and a.task_cxone_contact_id IS NOT NULL 
            AND  a.task_cxone_contact_id NOT IN ('',' ','0')) then 1
            when a.case_id = k.case_id AND a.taskid = k.taskid AND (a.task_owner_agent_name  in ('Customer Service Operations') and (a.task_cxone_contact_id IS NULL 
            OR  a.task_cxone_contact_id  IN ('',' ','0')))  then 1 else 0 end as penetration_calls_made
      ,tla.collections_action_flag
      ,case when a.lob = 'NAF' then NULL 
            when a.lob = 'OTR' AND upper(op.issuer_name) LIKE '%FLEET%ONE%' then 'FLEET ONE'
            when a.lob = 'OTR' AND (upper(op.issuer_name) NOT LIKE '%FLEET%ONE%' OR upper(op.issuer_name) IS NULL) then 'EFS' end as otr_platform_sub
      ,vic.scoringdt as var_scoring_dt
      ,vic.wx_days_past_due as platform_dpd
      ,vic.dpd_bucket as plaform_dpd_bucket
      ,vic.var_rate__c as var_rate__c 
      ,vic.var__c var__c
      ,vic.reasoncode1
      ,vic.reasoncode2
      ,vic.reasoncode3
      ,coalesce(mvarn.va_r,mvaro.va_r) as Task_level_var__c
      ,coalesce(mvarn.p_1,mvaro.p_1) as task_level_var_rate__c
      ,coalesce(mvarn.reasoncode1,mvaro.reasoncode1) as task_level_reasoncode1
      ,coalesce(mvarn.reasoncode2,mvaro.reasoncode2) as task_level_reasoncode2
      ,coalesce(mvarn.reasoncode3,mvaro.reasoncode3) as task_level_reasoncode3
      ,coalesce(NULLIF(ccqhd.var_q,' '),a.q_type) as task_level_var_q
      ,null as dialer_contact_method::varchar(100)
from pro_sandbox.ca_salesforce                 a
     left outer join
     pro_sandbox.ca_get_amt_due                   b
  on a.case_casenumber = b.case_casenumber
 and a.case_create_dt  = b.case_create_dt
      left outer join 
      pro_sandbox.ca_Promise_Payments e
  on a.ptpid = e.ptpid
     left outer join
     pro_sandbox.ca_sf_payments f
  on a.case_casenumber = f.case_casenumber
 and a.match_key       = f.match_key 
 and a.case_create_dt  = f.case_create_dt 
    left outer join
     naflt_psfin_rss.ps_customer      g
  on a.match_key = g.cust_id
     left outer join     
     naflt_dw_owner_rdw_rss.dw_acct   h
  on a.match_key = h.wex_acct_nbr  
     left outer join 
     pro_sandbox.ca_efs_customer_current i
  on a.match_key = i.cust_id
       left outer join
     pro_sandbox.ca_nice_acd1                         j
  on a.task_cxone_contact_id   = j.nice_contact_id
 and length(a.task_cxone_contact_id)>2 
    left outer join 
      pro_sandbox.ca_sf_task_penetration_calls           k
  on a.case_id = k.case_id
 AND a.taskid = k.taskid
 AND a.task_create_dttm = k.create_date
 AND k.taskid IS NOT NULL
     left outer join 
      pro_sandbox.ca_TaskLevelActionFlagC  tla
  on a.taskid = tla.taskid
 AND a.task_create_dttm = tla.task_create_dttm
 AND a.col_id = tla.hcolid
    left join pro_sandbox.ca_otr_platform1 op
  on a.match_key = op.ar_number
     left outer join pro_sandbox.ca_VarInfobyCase vic
  on a.case_id = vic.case_id 
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mvarn 
        on a.match_key = mvarn.cust_id 
       AND a.task_create_dt = mvarn.business_date 
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_otr mvaro 
        on a.match_key = mvaro.cust_id 
       AND a.task_create_dt = mvaro.business_date
    left outer join pro_sandbox.ca_collection_queue_hist ccqhd
        on a.case_id = ccqhd.id 
       AND a.task_create_dttm between ccqhd.fromdate_ts and ccqhd.todate_ts
--     AND ccqhd.intraday_row_number = 1
where a.lob in ('NAF','OTR');







update pro_sandbox.ca_collections_analytics1
set dialer_contact_method = ds.dialer_contact_method 
from pro_sandbox.ca_collections_analytics1 ca
inner join (SELECT ca.contact_id,
                case 
                    when s.outbound_strategy = 'Personal Connection' then 'Predictive'
                    when s.outbound_strategy = 'Manual' then 'Preview'
                    else s.outbound_strategy end as dialer_contact_method,
                cast(ca.contact_start as date) as contact_date9,
                min(ca.contact_start) as Min_Contactdate,
                max(ca.contact_start) as Max_Contactdate,
                count(*) as Ttl_Records
                from nice_acd_rss.completed_contact ca  
                left outer join nice_acd_rss.skill s
                on ca.skill_id = s.skill_id 
                group by ca.contact_id ,cast(ca.contact_start as date),s.outbound_strategy
) ds
on ca.task_cxone_contact_id = ds.contact_id;


--Updates the final dataset to clear out any promise to pay that was recorded by the agent NICE Integration.
update pro_sandbox.ca_collections_analytics1
set task_owner_agent_name = coalesce(case when task_owner_agent_name = 'NICE Integration' AND promise_kept = 1
then NULL else task_owner_agent_name end,ptp_agent);





drop table if exists pro_sandbox.ca_ptp_all1;
create table pro_sandbox.ca_ptp_all1 as
select 
sptp.id as ptpid,
sa.id as acctid,
sptp.createddate as ptp_create_dt,
sptpc.casenumber,
cc.q_type,
cc.lob,
coalesce(NULLIF(sa.wex_account__c,''),NULLIF(sc.ar_number__c,'')) as match_key,
sptp.collections__c as collectionid,
count(*) as nbr_of_installments,
min(coalesce(spp_all.payment_date__c,sptp.first_payment_date__c)) as ptp_first_payment_due_dt,
max(coalesce(
	case when 
		case when cast(spp_all.payment_date__c as date) <= cast(getdate() as date) then 'Y' else 'N' end ='Y' then cast(spp_all.payment_date__c as date) else NULL end, sptp.first_payment_date__c)) as ptp_last_payment_due_dt,
min(case when spp_all.payment_date__c IS NULL then sptp.first_payment_date__c
		 when cast(spp_all.payment_date__c as date) > cast(getdate() as date) then spp_all.payment_date__c 
	 end) as ptp_next_payment_due_dt,
max(coalesce(spp_all.payment_date__c,sptp.first_payment_date__c)) as ptp_end_payment_due_dt,
sum(NULLIF(spp_all.payment_amount__c,0.00)) as ptp_installment_amt_ttl,
max(sptp.payment_plan_total__c) as ptp_plan_ttl,
min(sptp.payment_plan_total__c) as min_ptp_plan_ttl,
sum(case when cast(coalesce(spp_all.payment_date__c,sptp.first_payment_date__c) as date) 
    <= cast(GETDATE() as date) then coalesce(spp_all.payment_amount__c,sptp.payment_plan_total__c,sptp.payment_amount__c,0) end) as ptp_last_amt,
sup.name as ptp_agent, 
'                                                   ' as promise_kept,
null::float as ttl_paid_to_promise -- new field added from latest alter table 2022-11-28
from 
salesforce_rss.sf_promise_to_pay sptp 
	left outer join salesforce_rss.sf_payment_plan spp_all
		on sptp.id = spp_all.promise_to_pay__c 
    left outer join salesforce_dl_rss."user" sup
		on sptp.createdbyid = sup.id
	left outer join salesforce_rss.sf_collections sc 
		on sptp.collections__c = sc.id 
	left outer join salesforce_rss.sf_case sptpc
		on sc.case__c = sptpc.id 
	left outer join salesforce_rss.sf_account sa 
		on sc.account__c = sa.id
	left outer join pro_sandbox.ca_collection_cases1 cc
		on sptpc.id = cc.case_id 
group by 
sptp.id,
sa.id,
sptp.createddate,
sptpc.casenumber,
cc.q_type,
cc.lob,
coalesce(NULLIF(sa.wex_account__c,''),NULLIF(sc.ar_number__c,'')),
sptp.collections__c,
sup.name;




drop table if exists pro_sandbox.ca_ptp_payments_all1;
create table pro_sandbox.ca_ptp_payments_all1 as 
select a.match_key
      ,a.ptpid
      ,max(b.payment_dt) as last_payment_dt_to_prmise
      ,sum(b.payment_amt)                   as ttl_paid_to_promise
      ,count(b.match_key)                   as nbr_of_pymts_to_promise 
      from pro_sandbox.ca_ptp_all1 a
	        left outer join 
	        pro_sandbox.ca_payments1   b
		  on a.match_key = b.match_key
		 and cast(b.payment_dt as date) BETWEEN cast(a.ptp_create_dt as date) AND cast(a.ptp_end_payment_due_dt as date)
group by a.match_key, a.ptpid;


update pro_sandbox.ca_ptp_all1 
set promise_kept = case
						when ptp_all.ptp_end_payment_due_dt <= cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) >= coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'Yes - Final'
						when ptp_all.ptp_end_payment_due_dt <= cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) < coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'No - Final'
						when ptp_all.ptp_end_payment_due_dt > cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) >= coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'Yes - In Progress'
						when ptp_all.ptp_end_payment_due_dt > cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) < coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'No - In Progress'
						else 'Unknown' end,
	ttl_paid_to_promise = coalesce(round(ptp_pay_all.ttl_paid_to_promise,2),0)
from 
pro_sandbox.ca_ptp_all ptp_all 
inner join pro_sandbox.ca_ptp_payments_all ptp_pay_all
on ptp_all.ptpid = ptp_pay_all.ptpid;


v_section := 20; 
call elt.logging(v_jobname, v_section);
