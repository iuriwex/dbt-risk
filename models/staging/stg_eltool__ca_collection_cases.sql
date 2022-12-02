with source as (
    select *
    from {{ ref('ca_collection_cases_snapshot')}}
), renamed as (
    select 
    case_casenumber
    ,match_key   
    ,case_account_status
    ,case_age_hours
    ,case_carrier_id   
    ,case_owner_name__c
    ,case_created_dttm
    case_create_dt   
    case_closed_dttm
    ,case_closed_dt         
    ,case_credit_limit
    ,driver_owner_name
    ,case_status
    ,primary_reason__c 
    ,case_secondary_reason
    ,case_brought_current
    ,case_rollup_past_due_amount
    ,col_past_due_amt
    ,col_id
    ,case_id
    ,case_dialer_status
    ,naf_cust_id
    ,wex_account
    ,Recourse_Code
    ,Collector_Code
    ,Account_Since
    ,Suspense_Class
    ,Language_Indicator
    ,Statement_Cycle
    ,PaymentMethod
    ,q_type
    ,platform__c
    ,lob
    ,contract_ar_number               
    ,col_statement_cycle 
    ,call_attempts_needed
    ,Follow_Up_Date_Current
    ,national_account_otr
    ,credit_national_account
    ,pfs_rep
    ,direct_debit
    ,task_level
    -- TODO: listing columns
    from source 
)
select *
from renamed