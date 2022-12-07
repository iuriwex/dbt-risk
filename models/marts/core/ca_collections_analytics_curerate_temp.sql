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
with table pro_sandbox.ca_collections_analytics_curerate_temp as (
    SELECT * FROM {{('stg_eltool__cal_collections_analytics_curerate_temp')}}
)
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
      left join naflt_rpt.nafleet_extended_terms b 
      on cdd.cust_id=b.customer
      left join reference_data.nafleet_tax_accounts tx 
      on cdd.cust_id = tx.customer
      left join (
         select T1.cust_id
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
      left outer join mktg_edw_rss.d_date   dt
      on business_date = dt.calendar_date_dt    
      where platform ='NA-FLEET' and setid<>'LLC' and pfs_rep =' ' and ar_total>0 and wx_days_past_due > 0 and b.customer  is NULL and tx.customer is NULL and business_date >= '2020-01-01' 
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
