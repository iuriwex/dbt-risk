with pro_sandbox.ca_collection_cases as (
    select * 
    from {{ ref('stg_eltool__ca_collection_cases') }}
)
select DISTINCT
    c.casenumber case_casenumber
    ,case when length(a.wex_account__c)>2 then a.wex_account__c else col.ar_number__c end as match_key   
    ,c.account_status__c              case_account_status
    ,c.age_hours__c                   case_age_hours
    ,c.carrier_id__c                  case_carrier_id   
    ,c.owner_name__c                  case_owner_name__c
    ,convert_timezone('UTC','EST',left(regexp_replace(c.createddate,'T',' '),19)::timestamp)               as case_created_dttm
    ,trunc(convert_timezone('UTC','EST',left(regexp_replace(c.createddate,'T',' '),19)::timestamp))        as case_create_dt   
    ,case when c.closeddate > ' ' then convert_timezone('UTC','EST',left(regexp_replace(c.closeddate,'T',' '),19)::timestamp) end  as case_closed_dttm
    ,case when c.closeddate > ' ' then trunc(convert_timezone('UTC','EST',left(regexp_replace(c.closeddate,'T',' '),19)::timestamp))
          else '9999-12-31' end  as case_closed_dt         
    ,c.credit_limit__c                case_credit_limit
    ,driver.owner_name__c             driver_owner_name
    ,c.status                         case_status
    ,c.primary_reason__c 
    ,c.secondary_reason__c            case_secondary_reason
    ,case when c.secondary_reason__c = 'Customer Current' then 'TRUE' else 'FALSE' end   as case_brought_current
    ,c.rollup_past_due_amount__c                case_rollup_past_due_amount
    ,col.past_due_amount__c         col_past_due_amt
    ,col.id                  col_id
    ,c.id                             case_id
    ,c.dialer_status__c               case_dialer_status
    ,a.acct_row_id__c                 naf_cust_id
    ,a.wex_account__c                 wex_account
    ,col.recourse_code__c as Recourse_Code
    ,col.collector_code__c as Collector_Code
    ,a.account_since__c as Account_Since
    ,col.suspense_class__c as Suspense_Class
    ,col.language_indicator__c as Language_Indicator
    ,col.statement_cycle__c as Statement_Cycle
    ,col.payment_method__c as PaymentMethod
    ,case when driver.owner_name__c like '%HVAR%'       then 'HVAR'
          when driver.owner_name__c like '%MVAR%'       then 'MVAR'
          when driver.owner_name__c like '%LVAR%'       then 'LVAR'
          when driver.owner_name__c like '%Self-cure%'  then 'SELFCURE'
          when driver.owner_name__c like '%Strategic%'  then 'STRATEGIC ACCOUNTS'
          when driver.owner_name__c like '%Outsourced%' then 'OUTSOURCED'  
          when driver.owner_name__c like '%Late Stage%' then 'LATE STAGE'         
          else 'OTHER' end as q_type
    ,a.platform__c
    ,case when a.platform_lob__c='N/A' and driver.owner_name__c like '%Agenda and Leaders Guide%' then driver.owner_name__c
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%Skill Based Training%'     then driver.owner_name__c
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%Factoring%'                then driver.owner_name__c     
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%NAF%'                      then 'NAF' 
          when a.platform_lob__c='N/A' and driver.owner_name__c like '%OTR%'                      then 'OTR'     
          else a.platform_lob__c 
         end as lob
    ,col.ar_number__c                 contract_ar_number               
    ,col.statement_cycle__c         col_statement_cycle 
    ,case when driver.owner_name__c LIKE ('%LVAR%') then 1
          when driver.owner_name__c LIKE ('%MVAR%') then 1
          when driver.owner_name__c LIKE ('%HVAR%') then 1
          else -1 end  as call_attempts_needed
    ,convert_timezone('UTC','EST',cast(NULLIF(c.follow_up_date__c,'') as datetime)) as Follow_Up_Date_Current
    ,a.national_account_otr__c as national_account_otr
    ,a.credit_national_account__c as credit_national_account
    ,a.pfs_rep__c as pfs_rep
    ,a.direct_debit__c as direct_debit
    ,0 as task_level
from pro_sandbox.ca_driver1 as driver
inner join salesforce_dl_rss.case c
    on c.id = driver.id
inner join salesforce_dl_rss.account a
    on c.accountid  = a.id 
left outer join salesforce_rss.sf_collections col
    on c.id        = col.case__c 
   AND c.accountid = col.account__c
 



 --
-- WHERE SHOULD I PUT THIS QUERY UPDATE? 
update pro_sandbox.ca_collection_cases
set task_level = 1 
from 
pro_sandbox.ca_collection_cases1 cccd
    inner join (select case_casenumber, case_id, min(col_id) as mincolid
    from pro_sandbox.ca_collection_cases1
    group by case_casenumber,case_id)  coltask
        on cccd.col_id = coltask.mincolid
      AND cccd.case_id = coltask.case_id;