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