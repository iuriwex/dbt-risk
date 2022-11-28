--Creates a dataset of the VAR Info by case for the output
--of the process.
with pro_sandbox.ca_VarInfobyCase as (
    SELECT *
    FROM {{('ca_VarInfoCase')}}
)
select 
vh.case_id,
vh.match_key,
vh.lob_table_source,
coalesce(cast(mnaf.partition_0 as date),cast(motr.partition_0 as date)) as scoringdt,
coalesce(mnaf.wx_days_past_due,motr.wx_days_past_due) as wx_days_past_due,
coalesce(mnaf.dpd_bucket,motr.dpd_bucket) as dpd_bucket,
coalesce(mnaf.p_1,motr.p_1) as var_rate__c,
coalesce(cast(NULLIF(mnaf.va_r,'') as decimal(18,2)),cast(NULLIF(motr.va_r,'') as decimal(18,2))) as var__c,
coalesce(mnaf.reasoncode1,motr.reasoncode1) as reasoncode1,
coalesce(mnaf.reasoncode2,motr.reasoncode2) as reasoncode2,
coalesce(mnaf.reasoncode3,motr.reasoncode3) as reasoncode3
from
pro_sandbox.ca_VARHistory vh
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mnaf
        on vh.match_key = mnaf.cust_id 
       AND vh.MaxScoringDt = cast(mnaf.partition_0 as date) 
       AND vh.lob_table_source = 'NAF'
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_OTR motr
        on vh.match_key = motr.cust_id 
           AND vh.MaxScoringDt = cast(motr.partition_0 as date)
           AND vh.lob_table_source = 'OTR';
