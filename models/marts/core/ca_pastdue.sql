/*  Nick Bogan, 2021-04-23: We create PRO_sandbox tables for the NAF and OTR past
 * due data, for better performance joining to ca_salesforce_driver. Also, we combine
 * the former ca_get_amtpastdue, ca_get_rev_amtpastdue and ca_get_rev_amtmindue
 * tables into one ca_get_amt_due table, to reduce joins when building
 * ca_collections_analytics. When we do so, we remove the measure fields from
 * these queries' GROUP BY clauses. For example, we don't group by
 * nvl(b.past_due_amount, c.past_due_total) in the first subquery. I don't see
 * why we would want separate rows for each measure amount in these data.
 */


-- ca_pastdue_naf
with pro_sandbox.ca_pastdue_naf as (
    SELECT * FROM {{('elt_tool__ca_pastdue_naf')}}
) 
select pd.cust_id,
    pd.business_date,
    pd.past_due_amount
from collections_history_prod_rss.nafleet_past_due as pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.nafleet_past_due as pd2);



-- ca_pastdue_otr
with pro_sandbox.ca_pastdue_otr as (
    SELECT * FROM {{('elt_tool__ca_pastdue_otr')}}
) 
select pd.ar_number,
    pd.ar_date,
    pd.past_due_total
from collections_history_prod_rss.otr_past_due pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.otr_past_due as pd2);



-- ca_get_amt_due
with pro_sandbox.ca_get_amt_due as (
    SELECT * FROM {{('elt_tool__ca_get_amt_due')}}
) 
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
from pro_sandbox.ca_salesforce_driver           a 
     left outer join pro_sandbox.ca_pastdue_naf b
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
 from pro_sandbox.ca_salesforce_driver a
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
 from pro_sandbox.ca_salesforce_driver1 a
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





