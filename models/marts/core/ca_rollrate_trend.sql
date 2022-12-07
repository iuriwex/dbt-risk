-Created 2020.01.11
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

with pro_sandbox.ca_rollrate_trend as (
    SELECT * FROM {{('stg_eltool__ca_roolrate_trend')}}
)
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
  inner join naflt_edw_rss.d_date     b
  on a.business_date = b.calendar_date_dt
  where b.calendar_date_dt between trunc(sysdate) - 210 and trunc(sysdate) and a.platform in ('NA-FLEET','TCHEK','EFSLLC') and b.last_business_day_in_month_flg = 'Yes'
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
left outer join getdata   b
on a.cust_id = b.cust_id and a.platform = b.platform and date_part('month',date_trunc('month',a.business_date))= date_part('month',date_trunc('month',b.business_date)-1)
left outer join dpd_bucket_counts  c
on a.platform      = c.platform and a.business_date = c.business_date and a.dpd_bucket    = c.dpd_bucket
where b.dpd_bucket > a.dpd_bucket 
group by a.platform, a.business_date, a.dpd_bucket, b.business_date, b.dpd_bucket   
order by a.platform, a.business_date, a.dpd_bucket, b.business_date, b.dpd_bucket

--End  3.DP-CollectionsAnalytics\Individual_Scripts\Roll Rate_PROD.sql
--Start 4.DP-CollectionsAnalytics\Individual_Scripts\Queue Migration_PROD.sql


/*
 * v_section := 40; 
 * call elt.logging(v_jobname, v_section);
*/

