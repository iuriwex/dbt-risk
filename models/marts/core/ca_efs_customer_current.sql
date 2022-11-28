/*  Nick Bogan, 2021-04-23: We build ca_efs_customer_current to simplify building
 * ca_collections_analytics. It's a *LOT* faster to use a max run date subquery
 * to get the run date than to use a window function over the whole table to find
 * the max run date. 
 */

with pro_sandbox.ca_efs_customer_current1 as (
    SELECT *
    FROM {{('elt_tool__ca_efs_cutomer_current')}}
)
select cast(customer_id as varchar) as cust_id,
--  customer_ID isn't unique in EFS_customer--platform is also required--but I see
-- no good platform to join on in the Salesforce data, so we pick the alphabetical
-- max platform to avoid duplication.
    max(platform) as platform
from efs_owner_crd_rss.efs_customer
where run_date =
    (select max(c.run_date) as max_run_date
    from efs_owner_crd_rss.efs_customer c)
group by customer_id;
