/*  Nick Bogan, 2021-04-23: The load of ca_salesforce_stagesort joined ca_collection_cases
 * and ca_collection_tasks using OR, which was very slow. We replace that with the
 * case_task subquery, a UNION ALL of three queries against the two tables (inner
 * join the first way, inner join the second [mutually exclusive] way, antijoin
 * both ways) with no logic in the joins, which is much faster.
 */

-- Creates a dataset that joins cases and tasks together, and that it will be used by ca_salesforce_stage and dropped afterwards
--  Do we even need this temooprary table? 
-- Instead,  we could run the select query below, directly to case_task and SORT it,  and add rownnumber into ca_sale

with pro_sandbox.ca_salesforce_stagesort as (
     SELECT * FROM {{('stg_eltool__ca_salesforce_stagesort')}}
)
select case_task.*,
    case when task_calldisposition in
            ('Promise to Pay',
            'Right Party Contact - Promise to Pay',
            'Right Party Contact - With Promise to Pay',
            'Right Party Contact w/ Promise to Pay',
            'Right Party Contact with pmt',
            'ptp')
         then 1 else 0 end as calldisp_ptpflg,
    0 as ptp_level
from -- case_task
    (select 'Case Task' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    inner join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.case_id = cctd.taskwhatid
       AND cccd.task_level = 1
    union all
    select 'Collection Task' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    inner join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.col_id = cctd.taskwhatid
       AND cccd.task_level = 1
    union all
    select 'Other' as Task_Assigned_To,
        cccd.*,
        cctd.*
    from pro_sandbox.ca_collection_cases1 cccd
    left outer join pro_sandbox.ca_collection_tasks1 cctd
        on cccd.case_id = cctd.taskwhatid
       AND cccd.task_level = 1
    left outer join pro_sandbox.ca_collection_tasks1 cctd_col
        on cccd.col_id = cctd_col.taskwhatid
       AND cccd.task_level = 1
    where cctd.taskwhatid is null
        and cctd_col.taskwhatid is null
    ) case_task;