--Creates a staging table of the var information for a specific collections id
--to be used for the next part of the process.
with pro_sandbox.ca_VARHistory as (
    SELECT *
    FROM {{('elt_tool_ca_VARHistory')}}
)
select
a.case_id,a.match_key,'NAF' as lob_table_source,max(cast(mnaf.partition_0 as date)) as MaxScoringDt,count(DISTINCT mnaf.business_date) as NbrOfScoringDays
from
pro_sandbox.ca_salesforce a
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mnaf
        on a.match_key = mnaf.cust_id 
where     
cast(mnaf.partition_0 as date) BETWEEN cast(a.case_created_dttm as date) AND cast(coalesce(a.case_closed_dttm,getdate()) as date) 
group by
a.case_id,a.match_key
UNION ALL 
select
a.case_id,a.match_key,'OTR' ,max(cast(motr.partition_0 as date)),count(DISTINCT motr.business_date) 
from
pro_sandbox.ca_salesforce a
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_OTR motr
        on a.match_key = motr.cust_id 
where     
cast(motr.partition_0 as date) BETWEEN cast(a.case_created_dttm as date) AND cast(coalesce(a.case_closed_dttm,getdate()) as date) 
group by
a.case_id,a.match_key;
