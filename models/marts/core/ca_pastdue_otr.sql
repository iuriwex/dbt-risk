-- ca_pastdue_otr
with pro_sandbox.ca_pastdue_otr as (
    SELECT * FROM {{('elt_tool__ca_pastdue_otr')}}
) 
select pd.ar_number,
    pd.ar_date,
    pd.past_due_total
from collections_history_prod_rss.otr_past_due pd
where pd.partition_0 =
    (select max(pd2.partition_0) as max_partition
    from collections_history_prod_rss.otr_past_due as pd2);


