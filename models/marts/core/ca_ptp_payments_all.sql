with pro_sandbox.ca_ptp_payments_all as (
    SELECT * 
    FROM {{('stg_eltool_ca_ptp_payments_all')}}
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



-- Where should I place the UPDATE sctipr below

{{ config(
	post_hook=["update pro_sandbox.ca_ptp_all
				set promise_kept = case
				when ptp_all.ptp_end_payment_due_dt <= cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) >= coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'Yes - Final'
				when ptp_all.ptp_end_payment_due_dt <= cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) < coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'No - Final'
				when ptp_all.ptp_end_payment_due_dt > cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) >= coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'Yes - In Progress'
				when ptp_all.ptp_end_payment_due_dt > cast(getdate() as date) AND coalesce(ptp_pay_all.ttl_paid_to_promise,0) < coalesce(ptp_all.ptp_last_amt,ptp_all.ptp_plan_ttl,ptp_all.ptp_installment_amt_ttl,0) then 'No - In Progress'
				else 'Unknown' end,
				ttl_paid_to_promise = coalesce(round(ptp_pay_all.ttl_paid_to_promise,2),0)
				from pro_sandbox.ca_ptp_all ptp_all
				inner join pro_sandbox.ca_ptp_payments_all ptp_pay_all
				on ptp_all.ptpid = ptp_pay_all.ptpid"]
 )}}
