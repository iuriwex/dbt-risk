with pro_sandbox.ca_collections_analytics_curerate as (
    SELECT * FROM {{('stg_eltool__ca_collections_analytics_curerate')}}
)
with sf_driver as (
select nvl(a.id, b.caseid) as id
      ,a.accountid         as accountid 
      ,b.caseid            as history_caseid
      ,max(nvl(case when a.owner_name__c not like 'Collections%' then null else a.owner_name__c end , 
               case when b.oldvalue like 'Collections%' then b.oldvalue else b.newvalue end)) as owner_name__c 
from salesforce_dl_rss.case            a
     full outer join
     salesforce_rss.sf_case_history    b
   on a.id = b.caseid
where a.owner_name__c like 'Collections%'
   or (b.oldvalue like 'Collections%' or b.newvalue like 'Collections%')
group by nvl(a.id, b.caseid), a.accountid, b.caseid
)
,salesforce as (
select
    case when length(b.wex_account__c)>2 then b.wex_account__c else con.ar_number__c end            as match_key
   ,trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp))  as case_create_dt
   ,dt.month_year_abbr
   ,case when driver.owner_name__c like '%HVAR%'       then 'HVAR'
         when driver.owner_name__c like '%MVAR%'       then 'MVAR'
         when driver.owner_name__c like '%LVAR%'       then 'LVAR'
         when driver.owner_name__c like '%Self-cure%'  then 'SELFCURE'
         when driver.owner_name__c like '%Strategic%'  then 'STRATEGIC ACCOUNTS'
         when driver.owner_name__c like '%Outsourced%' then 'OUTSOURCED'         
         else 'OTHER' end as q_type
from sf_driver                      driver
     inner join
     salesforce_dl_rss.case         a
  on driver.id = a.id   
     inner join 
     salesforce_dl_rss.account      b 
  on a.accountid = b.id
     left outer join 
     salesforce_rss.sf_contract__c  con 
  on con.account__c = b.id
     left outer join
     mktg_edw_rss.d_date             dt
  on trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp)) = dt.calendar_date_dt    
)
,queue_priority as (
select a.match_key
      ,a.month_year_abbr
      ,max(case when a.q_type='HVAR' then 1 else 0 end) as hvar
      ,max(case when a.q_type='MVAR' then 1 else 0 end) as mvar
      ,max(case when a.q_type='LVAR' then 1 else 0 end) as lvar      
      ,max(case when a.q_type='SELFCURE' then 1 else 0 end)           as sc      
      ,max(case when a.q_type='STRATEGIC ACCOUNTS' then 1 else 0 end) as sa
      ,max(case when a.q_type='OUTSOURCED' then 1 else 0 end)         as os      
  from salesforce  a
  group by a.match_key, a.month_year_abbr
)
,q_assignment as ( 
select a.*
      ,case when b.hvar=1 then 'HVAR'
            when b.mvar=1 then 'MVAR'
            when b.lvar=1 then 'LVAR'
            when b.sc=1   then 'SELFCURE'            
            when b.sa=1   then 'STRATEGIC ACCOUNTS'
            when b.os=1   then 'OUTSOURCED'            
        end as q_type                
 from pro_sandbox.ca_collections_analytics_curerate_temp  a
      left outer join
      queue_priority                                   b
   on a.cust_id = b.match_key
  and a.month_year_abbr = b.month_year_abbr
)
select *
from q_assignment
 where q_type is null or q_type in ('HVAR','MVAR','LVAR','SELFCURE')  
