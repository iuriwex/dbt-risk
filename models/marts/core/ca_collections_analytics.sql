--Creates final output that weaves a lot of the above tables together
--as Tableau cannot handle joins with data this large 
with pro_sandbox.ca_collections_analytics as (
    SELECT *
    FROM {{('elt_tool_ca_collections_analitycs')}}
)
select distinct 
       a.*
      ,case when g.setid='REV' then b.begin_rev_amtpastdue_dt else b.begin_amtpastdue_dt end as begin_amtpastdue_dt
      ,case when g.setid='REV' then b.end_rev_amtpastdue_dt   else b.end_amtpastdue_dt end   as end_amtpastdue_dt
      ,coalesce(case when g.setid='REV' then b.rev_amtpastdue_amt      
                     else b.amtpastdue_amt end,cast(NULLIF(a.case_rollup_past_due_amount,'') as decimal(18,2)))      as amtpastdue_amt 
      ,b.begin_rev_billing_dt
      ,b.end_rev_billing_dt
      ,b.rev_minamtdue_amt
      ,case when a.ptp_payment_amt <= e.ttl_paid_to_promise then TRUE else FALSE end as promise_kept
      ,e.ttl_paid_to_promise
      ,e.nbr_of_pymts_to_promise      
      ,f.begin_payment_dt
      ,f.end_payment_dt
      ,f.payment_amt      
      ,case when a.lob='OTR' then i.platform 
            when a.lob='NAF' then g.setid 
            else null end as business_unit    
      ,g.name1
      ,h.l1_acct_nm
      ,g.setid    as naf_setid      
      ,i.platform as otr_platform
      ,j.*
      ,k.call_attempts_needed as penetration_call_needed
      /* updating the logic behind penetration_calls_made to capture the genesys despotioned calls. The genesys despoitioned calls don't have a task_cxone_contact_id and the task_owner_agent_name 
      for all the Genesys despositioned calls is "Customer Service Operations"  */
      ,case when a.case_id = k.case_id AND a.taskid = k.taskid AND (a.task_owner_agent_name not in ('Customer Service Operations') and a.task_cxone_contact_id IS NOT NULL 
            AND  a.task_cxone_contact_id NOT IN ('',' ','0')) then 1
            when a.case_id = k.case_id AND a.taskid = k.taskid AND (a.task_owner_agent_name  in ('Customer Service Operations') and (a.task_cxone_contact_id IS NULL 
            OR  a.task_cxone_contact_id  IN ('',' ','0')))  then 1 else 0 end as penetration_calls_made
      ,tla.collections_action_flag
      ,case when a.lob = 'NAF' then NULL 
            when a.lob = 'OTR' AND upper(op.issuer_name) LIKE '%FLEET%ONE%' then 'FLEET ONE'
            when a.lob = 'OTR' AND (upper(op.issuer_name) NOT LIKE '%FLEET%ONE%' OR upper(op.issuer_name) IS NULL) then 'EFS' end as otr_platform_sub
      ,vic.scoringdt as var_scoring_dt
      ,vic.wx_days_past_due as platform_dpd
      ,vic.dpd_bucket as plaform_dpd_bucket
      ,vic.var_rate__c as var_rate__c 
      ,vic.var__c var__c
      ,vic.reasoncode1
      ,vic.reasoncode2
      ,vic.reasoncode3
      ,coalesce(mvarn.va_r,mvaro.va_r) as Task_level_var__c
      ,coalesce(mvarn.p_1,mvaro.p_1) as task_level_var_rate__c
      ,coalesce(mvarn.reasoncode1,mvaro.reasoncode1) as task_level_reasoncode1
      ,coalesce(mvarn.reasoncode2,mvaro.reasoncode2) as task_level_reasoncode2
      ,coalesce(mvarn.reasoncode3,mvaro.reasoncode3) as task_level_reasoncode3
      ,coalesce(NULLIF(ccqhd.var_q,' '),a.q_type) as task_level_var_q
      ,null as dialer_contact_method::varchar(100)
from pro_sandbox.ca_salesforce                 a
     left outer join
     pro_sandbox.ca_get_amt_due                   b
  on a.case_casenumber = b.case_casenumber
 and a.case_create_dt  = b.case_create_dt
      left outer join 
      pro_sandbox.ca_Promise_Payments e
  on a.ptpid = e.ptpid
     left outer join
     pro_sandbox.ca_sf_payments f
  on a.case_casenumber = f.case_casenumber
 and a.match_key       = f.match_key 
 and a.case_create_dt  = f.case_create_dt 
    left outer join
     naflt_psfin_rss.ps_customer      g
  on a.match_key = g.cust_id
     left outer join     
     naflt_dw_owner_rdw_rss.dw_acct   h
  on a.match_key = h.wex_acct_nbr  
     left outer join 
     pro_sandbox.ca_efs_customer_current i
  on a.match_key = i.cust_id
       left outer join
     pro_sandbox.ca_nice_acd1                         j
  on a.task_cxone_contact_id   = j.nice_contact_id
 and length(a.task_cxone_contact_id)>2 
    left outer join 
      pro_sandbox.ca_sf_task_penetration_calls           k
  on a.case_id = k.case_id
 AND a.taskid = k.taskid
 AND a.task_create_dttm = k.create_date
 AND k.taskid IS NOT NULL
     left outer join 
      pro_sandbox.ca_TaskLevelActionFlagC  tla
  on a.taskid = tla.taskid
 AND a.task_create_dttm = tla.task_create_dttm
 AND a.col_id = tla.hcolid
    left join pro_sandbox.ca_otr_platform1 op
  on a.match_key = op.ar_number
     left outer join pro_sandbox.ca_VarInfobyCase vic
  on a.case_id = vic.case_id 
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_naf mvarn 
        on a.match_key = mvarn.cust_id 
       AND a.task_create_dt = mvarn.business_date 
    left outer join collections_history_prod_rss.mckinsey_value_at_risk_otr mvaro 
        on a.match_key = mvaro.cust_id 
       AND a.task_create_dt = mvaro.business_date
    left outer join pro_sandbox.ca_collection_queue_hist ccqhd
        on a.case_id = ccqhd.id 
       AND a.task_create_dttm between ccqhd.fromdate_ts and ccqhd.todate_ts
--     AND ccqhd.intraday_row_number = 1
where a.lob in ('NAF','OTR');




-- TODO: Rewrite create query to suport the following update and upgrade

-- 
-- author Iuri <iuri.dearaujo@wexinc.com>  
-- creation_date: 20221128
-- Where aLTER tables must GO?
-- the scriupt to create table must be up to date to any DMLs 
--Alters the final dataset to add a column to store how the case
--was called by the NICE system.
-- ALTER TABLE pro_sandbox.ca_collections_analytics1
--    add dialer_contact_method varchar(100) null;



--TODO 
update pro_sandbox.ca_collections_analytics1
set dialer_contact_method = ds.dialer_contact_method 
from 
pro_sandbox.ca_collections_analytics1 ca
    inner join (SELECT 
                ca.contact_id,
                case 
                    when s.outbound_strategy = 'Personal Connection' then 'Predictive'
                    when s.outbound_strategy = 'Manual' then 'Preview'
                    else s.outbound_strategy end as dialer_contact_method,
                cast(ca.contact_start as date) as contact_date9,
                min(ca.contact_start) as Min_Contactdate,
                max(ca.contact_start) as Max_Contactdate,
                count(*) as Ttl_Records
                from nice_acd_rss.completed_contact ca  
                    left outer join nice_acd_rss.skill s
                        on ca.skill_id = s.skill_id 
                group by 
                ca.contact_id ,cast(ca.contact_start as date),s.outbound_strategy ) ds
        on ca.task_cxone_contact_id = ds.contact_id;




        
--Updates the final dataset to clear out any promise to pay that was recorded by the agent NICE Integration.
update pro_sandbox.ca_collections_analytics1
    set task_owner_agent_name = coalesce(case when task_owner_agent_name = 'NICE Integration' AND promise_kept = 1
                                              then NULL else task_owner_agent_name end,ptp_agent);