

/*  Nick Bogan, 2021-04-23: When we build ca_driver, we don't bother doing a full
 * outer join of case and sf_case_history, because ca_driver is inner joined to
 * case when loading ca_collection_cases.
 */

--Creates a dataset of all cases that were ever assigned to collections 
--to be used as a base population for the entirety of the collections
--data model/set creation


with pro_sandbox.ca_driver as (
    select * 
    from {{ ref('stg_eltool__ca_driver') }}
)
select a.id,
    max(case 
        when b.newvalue like 'Collections%' then b.newvalue 
        when b.oldvalue like 'Collections%' then b.oldvalue
        else a.owner_name__c end) as owner_name__c 
from salesforce_dl_rss.case            a
     left outer join
     salesforce_rss.sf_case_history    b
   on a.id = b.caseid
where (a.owner_name__c like 'Collections%' or b.oldvalue like 'Collections%' or b.newvalue like 'Collections%')
group by a.id