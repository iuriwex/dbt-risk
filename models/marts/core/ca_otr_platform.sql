--Creates a dataset to assign OTR accounts to Fleet One or EFSLLC
with pro_sandbox.ca_otr_platform as (
    SELECT * FROM {{'stg_eltool__ca_otr_platform'}}
)
select DISTINCT 
ar_number,
platform,
issuer_name
from
efs_owner_crd_rss.efs_ar_master eam ;