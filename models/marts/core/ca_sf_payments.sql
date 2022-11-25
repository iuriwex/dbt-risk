with pro_sandbox.ca_sf_payments as (
    SELECT * FROM {{('elt_tool__ca_sf_payments')}}
)
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