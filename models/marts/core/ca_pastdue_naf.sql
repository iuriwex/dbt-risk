/*  Nick Bogan, 2021-04-23: We create PRO_sandbox tables for the NAF and OTR past
 * due data, for better performance joining to ca_salesforce_driver. Also, we combine
 * the former ca_get_amtpastdue, ca_get_rev_amtpastdue and ca_get_rev_amtmindue
 * tables into one ca_get_amt_due table, to reduce joins when building
 * ca_collections_analytics. When we do so, we remove the measure fields from
 * these queries' GROUP BY clauses. For example, we don't group by
 * nvl(b.past_due_amount, c.past_due_total) in the first subquery. I don't see
 * why we would want separate rows for each measure amount in these data.
 */


-- ca_pastdue_naf
with pro_sandbox.ca_pastdue_naf as (
    SELECT * FROM {{('elt_tool__ca_pastdue_naf')}}
) 
select pd.cust_id,
    pd.business_date,
    pd.past_due_amount
from collections_history_prod_rss.nafleet_past_due as pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.nafleet_past_due as pd2);









