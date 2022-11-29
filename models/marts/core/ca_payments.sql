--Creates dataset of all the payments that have come in for OTR and NAF
with pro_sandbox.ca_payments as (
    SELECT * FROM {{('stg_eltool__ca_payments')}}
)
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
