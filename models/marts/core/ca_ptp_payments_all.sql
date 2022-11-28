with pro_sandbox.ca_ptp_payments_all1 as (
    SELECT * 
    FROM {{('elt_tool_ca_ptp_payments_all')}}
)
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



