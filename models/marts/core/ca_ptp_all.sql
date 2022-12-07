with pro_sandbox.ca_ptp_all as (
	SELECT *
	FROM {{('stg_eltool__ca_ptp_all')}}
)
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



-- Rewritten alter table to the origin create table script
-- alter table pro_sandbox.ca_ptp_all1
--	add column ttl_paid_to_promise float null;






-- End 1.DP-CollectionsAnalytics\Individual_Scripts\Create_Collection_Analytics_Datasets_Prod.sql
-- Start 2.DP-CollectionsAnalytics\Individual_Scripts\Segment Analysis Query_PROD
