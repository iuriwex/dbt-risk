--Creates dataset the unique promise to pay entries to payments
--that will be used in final output 
with pro_sandbox.ca_promise_payments as (
    SELECT * FROM {{('stg_eltool__ca_promise_payments')}}
)
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