/*  Nick Bogan, 2021-04-23: When loading ca_salesforce_stage, we use the fields
 * from the ORDER BY clause of the prior query to determine uniquerowid, then order
 * the results by uniquerowid. 
 */

--Sorts data and provides an ascending uniquerowid for staging purposes
with pro_sandbox.ca_salesforce_stage1 as (
    SELECT * FROM {{('etl_tool__ca_salesforce_stage')}}
)
select 
row_number() OVER (order by case_casenumber, calldisp_ptpflg, task_create_dttm) as uniquerowid,
*
from 
pro_sandbox.ca_salesforce_stagesort1
order by uniquerowid;




-- TODO: Rewrite create query to suport the following update and upgrade

-- 
-- author Iuri <iuri.dearaujo@wexinc.com>  
-- creation_date: 20221128
-- Where aLTER tables must GO?
-- the scriupt to create table must be up to date to any DMLs 
--Flags the appropriate level to attach a promise to pay record
update pro_sandbox.ca_salesforce_stage1
set ptp_level = 1
from 
pro_sandbox.ca_salesforce_stage1 cssd
    inner join
    (select case_id, col_id,
        max(case when calldisp_ptpflg = 1 then UniqueRowID else NULL end) as MaxUniqueRowID1,
        max(uniquerowid) as maxUniqueRowID
    from pro_sandbox.ca_salesforce_stage1
    group by case_id, col_id) b 
        on cssd.UniqueRowID = coalesce(b.MaxUniqueRowID1,b.MaxUniqueRowID);