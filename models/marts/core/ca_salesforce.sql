--Creates dataset that introduces promise to pay elements
with pro_sandbox.ca_salesforce as (
    SELECT * FROM {{('stg_eltool__ca_salesforce')}}
)
SELECT 
 cssd.*
,p.ptpid
,p.ptp_create_dt
,p.first_payment_date__c          ptp_first_payment_dt
,coalesce(p.payment_plan_total__c,p.payment_amount__c)  ptp_payment_amt
,p.payment_frequency__c           ptp_payment_freq
,p.payment_type__c                ptp_payment_type
,sup.name                         ptp_agent
,pp.days_since_promised_for__c    ptp_days_since_promised
,pp.payment_amount__c             pymtpln_payment_amount
,pp.payment_date__c               pymtpln_payment_date
,pp.payment_remitted__c           pymtpln_payment_remitted
,supp.name                        pymtpln_agent
FROM pro_sandbox.ca_salesforce_stage cssd
LEFT OUTER JOIN   
             (select collections__c,first_payment_date__c,payment_plan_total__c,payment_amount__c,payment_frequency__c,payment_type__c
             ,id as ptpid,cast(createddate as date) as ptp_create_dt,createdbyid,
             dense_rank() over(partition by collections__c order by row_created_ts desc) as first_payment_date_rankdesc          
             from salesforce_rss.sf_promise_to_pay
             )  p
        on cssd.col_id = p.collections__c
       AND p.first_payment_date_rankdesc = 1 
       AND cssd.ptp_level = 1 
    left outer join salesforce_rss.sf_payment_plan pp 
        on cssd.col_id = pp.collections__c
    left outer join salesforce_rss.sf_user sup
        on p.createdbyid = sup.id
    left outer join salesforce_rss.sf_user supp
        on pp.createdbyid  = supp.id