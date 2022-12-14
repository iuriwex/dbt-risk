

/***********************************************************************************/
/* Script create date: 2021.01.07 Report Author: NMorrill; PLiberatore             */
/* Script Audited by: MGanesh         Report Audited Date: 2021-01-06              */
/* Script Run Time: 30-60 minutes                                                  */
/* Schedule: Daily at 0700 EST (1300 UTC/GMT)                                      */
/*           OR when the salesforce_rss and naflt_psfin_rss finishes refreshing    */
/* Data source: entapps_stage                                                      */
/* Data schema salesforce_rss, naflt_psfin_rss, efs_owner_crd_rss,                 */
/*              collection_history_prod_rss, salesforce_dl_rss, pro_sandbox        */
/* Routine Purpose:                                                                */
/*    Creates data sets that are used in various collection tableau visualizations */
/*    These datasets are used to prove and measure the following topics:           */
/*    collections strategy success, collection agent success, customer payment     */
/*    events, customer risk elements, call and email events, collection agent      */
/*    productivity, intersystem intergration validations.                          */
/* Consumer Audience: Collections Leadership; Collection Team Members, Risk  Mgmt  */
/*                    ELT, Data Analytics, Data Visualizations                     */
/* Affliated Scripts:                                                              */
/*       Repo: DP-CollectionsAnalytics\Scheduled_Scripts  (Main Branch)            */
/*                 Script: SF_Collections_CallPlan_PROD.sql                        */
/*                 Script: NAFLT_Aging_Master_PROD.sql                             */
/*                 Script: EFS_Aging_Master_PROD.sql                               */
/*                                                                                 */
/* Notes: Datasets are very large due to the nature of the way the queries were    */
/*       originally designed and then modified to meet various business            */
/*       requrements. Query will have to be optimized over time to go from a daily */
/*       table reconstruct to an incremental load. Additionally, indexing may want */
/*       to be created to support this load to cut back the amount on run time.    */
/*       Eventually this should be converted to a true data Model.                 */
/*                                                                                 */
/*       We will want to keep staging tables for the time being in order to        */
/*       be able to trace issues that are found in stabilization. We should be     */
/*       able to drop most of these tables after the final output has been created */
/*       after stablization finishes.                                              */ 
/***********************************************************************************/

/******* ENABLE LOGS
v_section integer := 0;
v_tm timestamp;
v_jobname varchar(50) := 'run_collections_analytics_data_refresh';
v_section := 10; 
call elt.logging(v_jobname, v_section);
***/
/***
 * Change Log:
 ***/
-- This creates a dataset of the history of queue assignments for all collection cases
-- There is an intraday_row_number that assigns the value of 1 to the last queue that 
-- an account was assigned to at the end of the day. The max value of this column shows
-- the first unique value tha the account was assigned for for the day.
drop table if exists pro_sandbox.ca_collection_queue_hist1; 
create table pro_sandbox.ca_collection_queue_hist1 as
select
b.casenumber,
b.owner_name__c as current_caseowner_name,
nvl(a.caseid,b.id) as id,
a.createddate as fromdate_ts_utc, ------createddate is pulled as is inorder for the dense rank to function properly by taking the orginal timestamp into consideration
NULLIF(b.closeddate,' ') as caseclosed_ts_utc,----pulled in for validation, is not part of the final query
cast(NULLIF(b.closeddate,' ') as date) as caseclosed_dt_utc,----pulled in for validation, is not part of the final query
convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp) as fromdate_ts,
nvl(
    case when convert_timezone('UTC','EST',a.createddate) = lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate) 
         then lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate)
         else lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate)- interval '1 seconds' end,
case 
    when a.createddate > left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)
        then convert_timezone('UTC','EST',a.createddate)
    when NULLIF(b.closeddate,' ') IS NOT NULL
        then convert_timezone('UTC','EST',left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)::timestamp)
    when cast(NULLIF(b.closeddate,' ') as date)=cast(a.createddate as date)--- This is to satisfy the usecase where the case jumps multiple queue within the same day and closes on the same day as it was created. This logic ensures that, under such circumstances the to_date is same as the created date and not one day before so that these cases don't get omitted from the dataset.
        then convert_timezone('UTC','EST',a.createddate)
    else '9999-12-31' end) todate_ts,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
trunc(convert_timezone('UTC','EST',left(regexp_replace(a.createddate,'T',' '),19)::timestamp)) as fromdate,----Fromdate will act as pivot to make appropriate date joins with other tables
cast(nvl(lead(convert_timezone('UTC','EST',a.createddate),1) over (partition by casenumber order by a.createddate) - interval '1 seconds',
    case when NULLIF(b.closeddate,' ') < a.createddate
            then convert_timezone('UTC','EST',a.createddate)
         when NULLIF(b.closeddate,' ') IS NOT NULL
            then convert_timezone('UTC','EST',left(regexp_replace(NULLIF(b.closeddate,' '),'T',' '),19)::timestamp)
         when cast(NULLIF(b.closeddate,' ') as date)=cast(a.createddate as date)--- This is to satisfy the usecase where the case jumps multiple queue within the same day and closes on the same day as it was created. This logic ensures that, under such circumstances the to_date is same as the created date and not one day before so that these cases don't get omitted from the dataset.
            then convert_timezone('UTC','EST',a.createddate) 
         else '9999-12-31' end) as date) todate,--- to_date looks up the next queue change and takes one day before to indicate how long the case was in a particular queue before migration. It is also an integral part of the joins to other tables.
row_number() OVER(PARTITION BY a.caseid, cast(a.createddate as date) order by a.caseid, a.createddate desc) as IntraDay_Row_Number,
case
    when a.newvalue like 'Collections%' then a.newvalue
    else a.oldvalue end as value,
case 
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%NAF%' 
        then 'NAF'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%OTR%' 
        then 'OTR' end as lob,
case
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%HVAR%'
        then 'HVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%MVAR%'
        then 'MVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%LVAR%'
        then 'LVAR'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Self-cure%'
        then 'SELFCURE'
when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Strategic%'
        then 'STRATEGIC ACCOUNTS'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Late Stage%'
        then 'LATE STAGE'
    when case when a.newvalue like 'Collections%' then a.newvalue else a.oldvalue end like '%Outsourced%'
        then 'OUTSOURCED'
    else 'OTHER' end as var_q,
dense_rank() over(partition by b.casenumber
            order by a.createddate) as row_order
from
salesforce_rss.sf_case_history a
    left outer join salesforce_dl_rss.case b 
        on a.caseid = b.id
where
a.field = 'Owner'---Looks up the created date and other meta data only when the field is owner so that we are capturing the information only when then there is a queue migration.
and trunc(a.createddate)>= '2020-01-01'
and (a.oldvalue = 'Credit Monitoring'
or substring(a.oldvalue, 1, 10)= 'Collection'
or a.newvalue = 'Credit Monitoring'
or substring(a.newvalue, 1, 10)= 'Collection');
 
