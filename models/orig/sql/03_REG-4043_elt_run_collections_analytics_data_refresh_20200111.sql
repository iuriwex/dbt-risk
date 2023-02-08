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
DROP TABLE IF EXISTS pro_sandbox.ca_segments_dashboard1;
create table pro_sandbox.ca_segments_dashboard1 as (
	with dollar_presented as (
		select 
			q_type,
			lob,
			naf_setid,
			otr_platform_sub,
			case_created_dttm::date,
			count(case_casenumber) total_cases,
			sum(dollar_presented) dollar_presented
		from (
			select 
				case_casenumber,
				case_created_dttm::date,
				q_type,
				lob,
				naf_setid,
				otr_platform_sub,
				max(amtpastdue_amt) dollar_presented
			from pro_sandbox.ca_collections_analytics1
			--where setid='REV' and q_type='MVAR' and lob='NAF'
			group by case_casenumber, case_created_dttm::date, q_type, lob, naf_setid, otr_platform_sub
		)
		group by q_type, case_created_dttm::date, lob,	naf_setid,	otr_platform_sub
	),
	dollar_collected as (
		select 
			q_type,
			lob,
			naf_setid,
			otr_platform_sub,
			case_created_dttm::date,
			count(case_casenumber)total_closed,
			sum(dollar_collected) dollar_collected
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
			group by case_casenumber, case_created_dttm::date, q_type, lob,	naf_setid,	otr_platform_sub
		)
		group by q_type,	lob,naf_setid, otr_platform_sub, case_created_dttm::date
	)
	select 
		a.*,
		b.total_closed,
		b.dollar_collected
	from dollar_presented a 
	left join dollar_collected b
	on a.q_type=b.q_type and a.lob=b.lob and a.naf_setid=b.naf_setid and a.case_created_dttm::date=b.case_created_dttm::date
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
);

-- To understand dolar_presented and dolar_colected I need to run the script, create tables, analyze data.
-- @author Iuri iuir.dearaujo@wexinc.com
-- @creation_date 20221214



--End 2.DP-CollectionsAnalytics\Individual_Scripts\Segment Analysis Query_PROD.sql
--Start 3.DP-CollectionsAnalytics\Individual_Scripts\Roll Rate_PROD.sql

v_section := 30; 
call elt.logging(v_jobname, v_section);

--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates a dataset that captures roll rates for NAFLT and OTR(EFS_ programs.
--Dependant on: Nothing
--Schedule: Daily, 0800
--Run Time: 5-10 minutes

/**** Change Log ****/
/* 2021.02.23: Update join issues that are causing data not to be returned in query (date issue) */
/*             line +48                                                                          */
DROP TABLE IF EXISTS pro_sandbox.ca_rollrate_trend1;
create table pro_sandbox.ca_rollrate_trend1 as (
with getdata as (
select a.cust_id
      ,a.platform
      ,a.business_date
      ,a.wx_days_past_due
      ,max(case when a.wx_days_past_due > 0 and a.wx_days_past_due  <= 29   then 1
                when a.wx_days_past_due > 29 and a.wx_days_past_due <= 59   then 2 
                when a.wx_days_past_due > 59 and a.wx_days_past_due <= 89   then 3
                when a.wx_days_past_due > 89 and a.wx_days_past_due <= 119  then 4
                else 0
            end) as dpd_bucket
from wex_rpt.collection_db_dm a
     inner join
     naflt_edw_rss.d_date     b
  on a.business_date = b.calendar_date_dt
where b.calendar_date_dt between trunc(sysdate) - 210 and trunc(sysdate)
  and a.platform in ('NA-FLEET','TCHEK','EFSLLC')
 and b.last_business_day_in_month_flg = 'Yes'
group by a.cust_id, a.platform, a.business_date,a.wx_days_past_due
)
,dpd_bucket_counts as (
select a.platform
      ,a.business_date
      ,a.dpd_bucket,
      date_trunc('month',business_date),
      date_part('month',date_trunc('month',business_date)-1)
      ,count(distinct a.cust_id) as total_customers
 from getdata a
 group by a.platform, a.business_date, a.dpd_bucket,extract(month from business_date),
      extract(month from business_date)-1
)
--select * from dpd_bucket_counts;
select a.platform
      ,max(c.total_customers)    as total_from_customers
      ,a.business_date           as from_date
      ,a.dpd_bucket              as from_dpdbucket      
      ,b.business_date           as to_date
      ,b.dpd_bucket              as to_dpdbucket
      ,count(distinct a.cust_id) as customers
      ,cast(count(distinct a.cust_id) as dec) / cast(max(c.total_customers) as dec) as pct
  from getdata   a
       left outer join
       getdata   b
    on a.cust_id = b.cust_id
   and a.platform = b.platform
   and date_part('month',date_trunc('month',a.business_date))= date_part('month',date_trunc('month',b.business_date)-1)
      left outer join
       dpd_bucket_counts  c
    on a.platform      = c.platform
   and a.business_date = c.business_date
   and a.dpd_bucket    = c.dpd_bucket
 where b.dpd_bucket > a.dpd_bucket
group by a.platform, a.business_date, a.dpd_bucket, b.business_date, b.dpd_bucket   
order by a.platform, a.business_date, a.dpd_bucket, b.business_date, b.dpd_bucket);
--End  3.DP-CollectionsAnalytics\Individual_Scripts\Roll Rate_PROD.sql
--Start 4.DP-CollectionsAnalytics\Individual_Scripts\Queue Migration_PROD.sql


/*
 * v_section := 40; 
 * call elt.logging(v_jobname, v_section);
*/




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
DROP TABLE IF EXISTS pro_sandbox.ca_queue_migration1;
create table pro_sandbox.ca_queue_migration1 as (
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
       left outer join
       salesforce_dl_rss.case         b
    on a.caseid = b.id    
       left outer join
       salesforce_dl_rss.account      c
    on b.accountid = c.id       
       left outer join 
       salesforce_rss.sf_contract__c  d
    on c.id = d.account__c
 left outer join 
 salesforce_rss.sf_user e 
 on 
 a.createdbyid=e.id
 where b.casenumber in (select distinct case_casenumber from pro_sandbox.ca_collections_analytics) 
and a.field='Owner'
   and trunc(a.createddate)>= '2020-01-01'
   and (a.oldvalue='Credit Monitoring' or substring(a.oldvalue,1,10)='Collection' or
        a.newvalue='Credit Monitoring' or substring(a.newvalue,1,10)='Collection'))
       -- and b.casenumber='04896848')
,migration as (
select a.casenumber,
b.createddate
      ,a.created_dt                      as from_queue_date
      ,nvl(b.created_dt,'9999-12-31')    as to_queue_date
      ,a.row_order,
      nvl(a.lob,b.lob) lob,
      a.var_q  as from_q
      ,b.var_q  as to_q,
      b.name
 from fulldata    a
      left outer join
      fulldata    b
   on a.casenumber = b.casenumber 
  and a.row_order = b.row_order - 1
 where nvl(b.created_dt,'9999-12-31') != '9999-12-31'  
order by a.casenumber, a.createddate) 
select *
from migration  a
where a.from_q != a.to_q
  and a.from_queue_date <= a.to_queue_date
order by a.casenumber, a.from_queue_date);
--End 4.DP-CollectionsAnalytics\Individual_Scripts\Queue Migration_PROD.sql
--Start 5.DP-CollectionsAnalytics\Individual_Scripts\Cure Rates_PROD.sql

/* uncoment later 
v_section := 50; 
call elt.logging(v_jobname, v_section);
*/






--Created 2020.01.11
--Created By: MGanesh;
--Audited By: NMorrill
--Audited On: 2020.01.11
--Purpose: Creates Cure Rates Dataset that will be used for collection analysis and visualization
--Dependant on: None
--Schedule: Daily, 0800
--Run Time: 5-10 minutes

/*** Change Log:
 *    Updated create table pro_sandbox.ca_collection_analytics_currate
 *    to pull off of ca_collections_analytics_current_temp instead of a
 *    development table. NAM  20210115
 */
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------
drop table if exists pro_sandbox.ca_collections_analytics_curerate_temp1;
create table pro_sandbox.ca_collections_analytics_curerate_temp1 as (
select 'NAF' as lob
      ,month_year_abbr
      ,frd_bk
      ,cust_id
      ,business_date
      ,ar_total
      ,platform
      ,business_unit
      ,setid
      ,concat(cust_id, concat_cure_dt)    as delinquent_instances_in_month
      ,concat(cust_id, first_cured_date)  as cured_instances_in_month      
from (
    with bs as (
      select substring(business_date,1,7) as yr_mo
            ,dt.month_year_abbr
            ,cdd.cust_id
            ,wx_days_past_due
            ,business_date
            ,ar_total
           ,platform
           ,setid
           ,business_unit
            ,case when frd_bk.cust_id is not null then 1 else 0 end as frd_bk
       from stage.collection_db_dm cdd
            left join 
            naflt_rpt.nafleet_extended_terms b 
         on cdd.cust_id=b.customer
            left join reference_data.nafleet_tax_accounts tx 
         on cdd.cust_id = tx.customer
            left join (select T1.cust_id
                            ,t1.post_dt
                       from naflt_psfin_rss.ps_item_activity             T1
                            left join naflt_psfin_rss.ps_sp_buared_clsvw T2 on T1.business_unit = T2.business_unit
                            left join naflt_psfin_rss.ps_wx_customer_wex T3 on T1.cust_id = T3.cust_id 
                            left join naflt_edw_rss.d_account_current_vw T4 on T1.cust_id = T4.source_account_id
                      where t1.post_dt >= '2020-01-01'
                        and T1.group_type IN ('P','X','M')
                        and T1.business_unit IN ('CHVWO','EXXWO','FSCWO','IOLWO','ISBWO','WXBWO','WXCWO','REVWO')
                        and T2.oprclass = 'OBJQWEX'
                        and t1.entry_amt > 0
                        and t1.origin_id = 'XFER'
                        and t3.wx_rcrse_code IN ('90','LB','L0','LP','92','LI','LL')  -- fraud / bankruptcy
                      group by T1.cust_id, t1.post_dt
                       ) as frd_bk 
                    on cdd.cust_id=frd_bk.cust_id 
                   and cdd.business_date <= frd_bk.post_dt
              left outer join
              mktg_edw_rss.d_date   dt
           on business_date = dt.calendar_date_dt    
       where platform ='NA-FLEET' 
         and setid<>'LLC' 
         and pfs_rep =' ' 
         and ar_total>0 
         and wx_days_past_due > 0 
         and b.customer  is NULL 
         and tx.customer is NULL
         and business_date >= '2020-01-01' 
    )
    select bs.*       
          ,min(cdd.business_date) as first_cured_date
          ,case when min(cdd.business_date) is NULL then '1900-01-01' else min(cdd.business_date) end as concat_cure_dt
      from bs bs 
           left join 
           stage.collection_db_dm cdd 
        on bs.cust_id=cdd.cust_id 
       and bs.business_date <= cdd.business_date 
       and substring(cdd.business_date,1,7)=bs.yr_mo
       and cdd.wx_days_past_due = 0
    group by bs.cust_id, bs.wx_days_past_due, bs.business_date, bs.ar_total, bs.yr_mo, bs.frd_bk, bs.month_year_abbr, bs.platform, bs.setid, bs.business_unit 
    ) 

-------------------------------------------------------------------
UNION    
-------------------------------------------------------------------

--OTR
                      
select 'OTR' as lob
      ,month_year_abbr
      ,frd_bk
      ,cust_id
      ,business_date
      ,ar_total
      ,platform
      ,setid      
      ,business_unit
      ,concat(cust_id, concat_cure_dt)    as delinquent_instances_in_month
      ,concat(cust_id, first_cured_date)  as cured_instances_in_month      
  from (
      with bs as (
        select substring(business_date,1,7) as yr_mo
              ,cdd.cust_id
              ,wx_days_past_due
              ,business_date
              ,dt.month_year_abbr
              ,ar_total
             ,platform
             ,setid 
             ,business_unit
              ,case when frd_bk.cust_id is not null then 1 else 0 end as frd_bk
          from stage.collection_db_dm cdd
               left join  (select distinct cust_id
                               from (select cast(a.customer_id as varchar) as cust_id
                                        from efs_informix_ogden_rss.contract       as C
                                             left outer join 
                                             efs_informix_ogden_rss.contract_queue as cq 
                                          on cq.contract_id = c.contract_id
                                             left outer join 
                                             efs_informix_ogden_rss.credit_queue   as q 
                                          on q.queue = cq.queue
                                             inner join 
                                             efs_owner_crd_rss.efs_ar_master       as a 
                                          on trim(c.ar_number) = a.ar_number
                                       where q.description in ('National Acct','Partner Billing')
                                       UNION
                                       select cast(a.ar_number as varchar) as cust_id
                                         from efs_informix_ogden_rss.contract            as C
                                              left outer join 
                                              efs_informix_ogden_rss.contract_queue      as cq 
                                           on cq.contract_id = c.contract_id
                                              left outer join 
                                              efs_informix_ogden_rss.credit_queue        as q 
                                           on q.queue = cq.queue
                                              inner join 
                                              efs_owner_crd_rss.efs_ar_master            as a 
                                           on trim(c.ar_number) = a.ar_number
                                        where q.description in ('National Acct','Partner Billing')
                                        UNION
                                        select distinct cast(a.customer_id as varchar) as cust_id
                                          from efs_owner_crd_rss.efs_customer a
                                         where a.platform='TCHEK'
                                           and length(a.wex_national_id)>1
                                        UNION
                                        select ar_number as cust_id 
                                          from pro_sandbox.mck_exclusions_20200616 
                                          where spreadsheet <>'Jimmy'
                                        UNION
                                        select cast(customer_id as varchar) as cust_id 
                                        from pro_sandbox.mck_exclusions_20200616 
                                        where spreadsheet <>'Jimmy'
                                        UNION
                                        select ar_number as cust_id 
                                        from pro_sandbox.mckinsey_otr_national_account
                                     )
                                ) na 
                           on cdd.cust_id =na.cust_id
               left join (select distinct cust_id from (select ar_number as cust_id 
                                                          from pro_sandbox.mck_exclusions_20200616 
                                                         where spreadsheet ='Jimmy'
                                                            UNION 
                                                            select cast(customer_id as varchar) as cust_id 
                                                              from pro_sandbox.mck_exclusions_20200616 
                                                             where spreadsheet ='Jimmy')
                                                            UNION
                                                            select distinct cast(a.customer_id as varchar) as cust_id
                                                              from efs_informix_ogden_rss.contract as C
                                                              left outer join 
                                                              efs_informix_ogden_rss.contract_queue as cq 
                                                            on cq.contract_id = c.contract_id
                                                               left outer join efs_informix_ogden_rss.credit_queue as q 
                                                            on q.queue = cq.queue
                                                               inner join efs_owner_crd_rss.efs_ar_master  as a 
                                                            on trim(c.ar_number) = a.ar_number
                                                         where q.description in ('National Acct','Partner Billing')
                                                         UNION
                                                         select distinct cast(a.ar_number as varchar) as cust_id
                                                          from efs_informix_ogden_rss.contract as C
                                                          left outer join efs_informix_ogden_rss.contract_queue as cq 
                                                       on cq.contract_id = c.contract_id
                                                          left outer join efs_informix_ogden_rss.credit_queue as q 
                                                       on q.queue = cq.queue
                                                          inner join efs_owner_crd_rss.efs_ar_master  as a 
                                                       on trim(c.ar_number) = a.ar_number
                                                          where q.description in ('National Acct','Partner Billing')
                                                     ) jcl 
                                                 on jcl.cust_id=cdd.cust_id
                                     left join pro_sandbox.mckinsey_otr_direct_bill as nsacc 
                                  on cdd.cust_id=nsacc.ar_number -- this is the pilot group
                                     left join (select case when a.platform='EFSLLC' then a.ar_number
                                                           when a.platform='TCHEK'  then cast(a.customer_id as varchar)
                                                           else cast(a.customer_id as varchar) end as cust_id
                                                     ,min(a.chargeoff_date) as post_dt
                                                from efs_owner_crd_rss.efs_chargeoffs A         
                                                where a.chargeoff_date >= '2020-01-01'
                                                  and UPPER(SUBSTRING(a.CHARGEOFF_REASON,1,5)) = 'FRAUD' -- in ('FRAUD', 'BAD D', 'BADDE')
                                                  and a.chargeoff_amount*-1 > 0
                                                group by case when a.platform='EFSLLC' then a.ar_number
                                                              when a.platform='TCHEK'  then cast(a.customer_id as varchar)
                                                              else cast(a.customer_id as varchar) end                    
                                                   ) as frd_bk 
                                              on cdd.cust_id=frd_bk.cust_id 
                                             and cdd.business_date <= frd_bk.post_dt
                  left outer join
              mktg_edw_rss.d_date   dt
           on business_date = dt.calendar_date_dt                     
           where platform in ('EFSLLC','TCHEK')  
             and ar_total>0 
             and wx_days_past_due >0 
             and jcl.cust_id is NULL 
             and na.cust_id is NULL 
             and nsacc.ar_number is NULL
             and business_date >= '2020-01-01'
    )
    select bs.*
          ,min(cdd.business_date) as first_cured_date
          ,case when min(cdd.business_date) is NULL then '1900-01-01' else min(cdd.business_date) end as concat_cure_dt
      from bs bs 
           left join 
           stage.collection_db_dm cdd 
        on bs.cust_id=cdd.cust_id 
       and bs.business_date <= cdd.business_date 
       and substring(cdd.business_date,1,7)=bs.yr_mo
       and cdd.wx_days_past_due <=0
    group by bs.cust_id, bs.wx_days_past_due, bs.business_date, bs.ar_total, bs.yr_mo, bs.frd_bk, bs.month_year_abbr, bs.platform, bs.setid, bs.business_unit
    ) 
);





drop table if exists pro_sandbox.ca_collections_analytics_curerate1;
create table pro_sandbox.ca_collections_analytics_curerate1 as (
with sf_driver as (
select nvl(a.id, b.caseid) as id
      ,a.accountid         as accountid 
      ,b.caseid            as history_caseid
      ,max(nvl(case when a.owner_name__c not like 'Collections%' then null else a.owner_name__c end , 
               case when b.oldvalue like 'Collections%' then b.oldvalue else b.newvalue end)) as owner_name__c 
from salesforce_dl_rss.case            a
     full outer join
     salesforce_rss.sf_case_history    b
   on a.id = b.caseid
where a.owner_name__c like 'Collections%'
   or (b.oldvalue like 'Collections%' or b.newvalue like 'Collections%')
group by nvl(a.id, b.caseid), a.accountid, b.caseid
)
,salesforce as (
select
    case when length(b.wex_account__c)>2 then b.wex_account__c else con.ar_number__c end            as match_key
   ,trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp))  as case_create_dt
   ,dt.month_year_abbr
   ,case when driver.owner_name__c like '%HVAR%'       then 'HVAR'
         when driver.owner_name__c like '%MVAR%'       then 'MVAR'
         when driver.owner_name__c like '%LVAR%'       then 'LVAR'
         when driver.owner_name__c like '%Self-cure%'  then 'SELFCURE'
         when driver.owner_name__c like '%Strategic%'  then 'STRATEGIC ACCOUNTS'
         when driver.owner_name__c like '%Outsourced%' then 'OUTSOURCED'         
         else 'OTHER' end as q_type
from sf_driver                      driver
     inner join
     salesforce_dl_rss.case         a
  on driver.id = a.id   
     inner join 
     salesforce_dl_rss.account      b 
  on a.accountid = b.id
     left outer join 
     salesforce_rss.sf_contract__c  con 
  on con.account__c = b.id
     left outer join
     mktg_edw_rss.d_date             dt
  on trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp)) = dt.calendar_date_dt    
)
,queue_priority as (
select a.match_key
      ,a.month_year_abbr
      ,max(case when a.q_type='HVAR' then 1 else 0 end) as hvar
      ,max(case when a.q_type='MVAR' then 1 else 0 end) as mvar
      ,max(case when a.q_type='LVAR' then 1 else 0 end) as lvar      
      ,max(case when a.q_type='SELFCURE' then 1 else 0 end)           as sc      
      ,max(case when a.q_type='STRATEGIC ACCOUNTS' then 1 else 0 end) as sa
      ,max(case when a.q_type='OUTSOURCED' then 1 else 0 end)         as os      
  from salesforce  a
  group by a.match_key, a.month_year_abbr
)
,q_assignment as ( 
select a.*
      ,case when b.hvar=1 then 'HVAR'
            when b.mvar=1 then 'MVAR'
            when b.lvar=1 then 'LVAR'
            when b.sc=1   then 'SELFCURE'            
            when b.sa=1   then 'STRATEGIC ACCOUNTS'
            when b.os=1   then 'OUTSOURCED'            
        end as q_type                
 from pro_sandbox.ca_collections_analytics_curerate_temp1  a
      left outer join
      queue_priority                                   b
   on a.cust_id = b.match_key
  and a.month_year_abbr = b.month_year_abbr
)
select *
  from q_assignment
 where q_type is null or q_type in ('HVAR','MVAR','LVAR','SELFCURE')  
);
--End 5.DP-CollectionsAnalytics\Individual_Scripts\Cure Rates_PROD.sql
--Start 6.DP-CollectionsAnalytics\Individual_Scripts\Collections- New Data Model_PROD.sql




/* uncomment LOGS later 
* v_section := 60; 
* call elt.logging(v_jobname, v_section);
*/
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
DROP TABLE IF EXISTS pro_sandbox.ca_collections_analytics_audit_data_model1;
create table pro_sandbox.ca_collections_analytics_audit_data_model1 as (
----Driver CTE gets all possible date values by using task and case created date)
   with driver as (
      select
      distinct 
      cast(DATE_TRUNC('day',a.task_create_dttm) as date) date_driver 
      from pro_sandbox.ca_collections_analytics1 a 
      union 
      select 
      distinct 
      cast(DATE_TRUNC('day',b.case_created_dttm) as date) date_driver 
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
            else '9999-12-31' 
      end) todate,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
      case
         when a.newvalue like 'Collections%' then a.newvalue
         else a.oldvalue end as value
         ,case when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%NAF%' then 'NAF'
               when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%OTR%' then 'OTR'
         end as lob
         ,case 
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%HVAR%'       then 'HVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%MVAR%'       then 'MVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%LVAR%'       then 'LVAR'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Self-cure%'  then 'SELFCURE'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Returned Payment%'  then 'RETURN PAYMENT'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Outsourced%'  then 'OUTSOURCED'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Late Stage%'  then 'LATE STAGE'
            when (case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end) like '%Strategic Accounts%'  then 'STRATEGIC ACCOUNTS'
            else 'OTHER(DEFAULT, SOLD ACCOUNTS,FOLLOW UP)' 
         end as var_q
         ,dense_rank() over(partition by b.casenumber order by a.createddate) as row_order
      from salesforce_rss.sf_case_history a
      left outer join salesforce_dl_rss.case b
      on a.caseid = b.id
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
      or substring(a.newvalue, 1, 10)= 'Collection')
)
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
from driver a 
left join qtype b
on a.date_driver >= b.fromdate and a.date_driver <=b.todate ------ This join ensures that we are capturing the historic information on a daily basis for every case.
left join pro_sandbox.ca_collections_analytics1 c
on b.casenumber=c.case_casenumber
and cast(DATE_TRUNC('day',task_create_dttm) as date)=date_driver
left join (
   select case_casenumber
      ,case
         when case_closed_dttm is not null then cast(case_closed_dttm as date)
         else '9999-12-31' 
      end as case_closed_date
      ,case_secondary_reason,case_brought_current
   from pro_sandbox.ca_collections_analytics1
   group by case_casenumber,case_created_dttm,
   case 
      when case_closed_dttm is not null then cast(case_closed_dttm as date)
      else '9999-12-31'
   end
   ,case_secondary_reason,case_brought_current
) d 
on b.casenumber=d.case_casenumber and a.date_driver=d.case_closed_date
left join (
   select
      case_casenumber,
      max(amtpastdue_amt)amtpastdue_amt,
      q_type
   from pro_sandbox.ca_collections_analytics1
   group by case_casenumber, q_type
) e 
on b.casenumber=e.case_casenumber
left join (
   select casenumber, min(fromdate) created_date 
   from qtype 
   group by casenumber) f
on b.casenumber=f.casenumber and a.date_driver=f.created_date
--where 
--fromdate='2020-07-08' and 
--casenumber is not null
group by a.date_driver, b.casenumber, b.current_caseowner_name, b.id, b.var_q, b.fromdate,b.todate,b.lob,c.task_create_dttm,c.task_ae_activity_type,c.task_activity_type,c.task_owner_agent_name,c.task_cxone_contact_id,c.penetration_calls_made,c.task_disposition_name,c.task_calldisposition,d.case_secondary_reason,d.case_brought_current);
--End 6.DP-CollectionsAnalytics\Individual_Scripts\Collections- New Data Model_PROD.sql
--Start 7.DP-COllectionsAnalytics\Individual_Scripts\ACR_ Daily count_PROD.sql


/* uncomment later 
 * v_section := 70; 
 * call elt.logging(v_jobname, v_section);
*/

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
DROP TABLE IF EXISTS pro_sandbox.ca_acr_daily_callcount1;
create table pro_sandbox.ca_acr_daily_callcount1 as (
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
select * from primary_skill_assignment);


--End 7.DP-COllectionsAnalytics\Individual_Scripts\ACR_ Daily count_PROD.sql
--Start 8.DP-COllectionsAnalytics\Individual_Scripts\ACR_ dataset_PROD.sql

/* uncomment later
v_section := 80; 
call elt.logging(v_jobname, v_section);
*/


Created 2020.01.11
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
DROP TABLE IF EXISTS pro_sandbox.ca_acr_dataset1;
create TABLE pro_sandbox.ca_acr_dataset1 as (
   with agentcount as (
      select 
         task_create_date,
         primary_skill,
         lob,
         count(distinct agent_name) agent_count
      from pro_sandbox.ca_acr_daily_callcount1
      where primary_skill is not null
      group by task_create_date,lob,primary_skill
   ),
   casescount as (
      select 
         date_driver,
         var_q,
         lob,
         count(distinct casenumber) nbr_open_cases
      from pro_sandbox.ca_collections_analytics_audit_data_model1
      group by date_driver,var_q,lob
   )
   SELECT
      a.date_driver,
      a.var_q,
      a.lob,
      a.nbr_open_cases,
      b.agent_count
   from casescount a 
   left join agentcount b
   on a.date_driver=b.task_create_date and a.var_q=b.primary_skill
   and a.lob=b.lob
);

--End 6.DP-CollectionsAnalytics\Individual_Scripts\Collections- New Data Model_PROD.sql
--Start 7.DP-COllectionsAnalytics\Individual_Scripts\ACR_ Daily count_PROD.sql


v_section := 70; 
call elt.logging(v_jobname, v_section);
---------------------------------------------------------------------------
---------------------------------------------------------------------------
--add grants



--End 8.DP-COllectionsAnalytics\Individual_Scripts\ACR_ dataset_PROD.sql

---------------------------------------------------------------------------
--add grants
grant select on pro_sandbox.ca_collection_queue_hist1                    to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_driver1                                   to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collection_cases1                         to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collection_tasks1                         to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_salesforce_stage1                         to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_salesforce1                               to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_salesforce_driver1                        to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_get_amt_due1                              to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_payments1                                 to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_sf_payments1                              to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_ptp_unique1                               to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_promise_payments1                         to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_otr_platform1                             to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_nice_acd1                                 to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_sf_task_penetration_calls_stage1          to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_sf_task_penetration_calls1                to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_ColHistoryTaskDt1                         to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_TaskLevelActionFlagC1                     to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_VARHistory1                               to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_VarInfobyCase1                            to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_efs_customer_current1                     to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collections_analytics1                    to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_segments_dashboard1                       to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_rollrate_trend1                           to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_queue_migration1                          to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collections_analytics_curerate_temp1      to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collections_analytics_curerate1           to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_collections_analytics_audit_data_model1   to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_acr_daily_callcount1                      to group "role-g-entapps-redshift-analytics";
grant select on pro_sandbox.ca_acr_dataset1                              to group "role-g-entapps-redshift-analytics";

select convert_timezone('America/New_York', sysdate) into v_tm; 
RAISE NOTICE 'Completed script at %', v_tm;

v_section := 9999999; 
call elt.logging(v_jobname, v_section);

commit;

/* EXCEPTION
  WHEN OTHERS THEN
        RAISE EXCEPTION 'Failure in section %', v_section; */

