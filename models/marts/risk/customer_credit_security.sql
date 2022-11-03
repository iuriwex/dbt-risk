with efs_credit_security as (
    select *
    from {{ ref('fct_credit_security') }}
),
efs_customer as (
    select *
    from {{ ref('dim_customer') }}
)
select 
    cs.CREDITLINEID,
	cs.SECURITYTYPESEQUENCENUMBER,
	cs.SECURITYTYPECODE,
	cs.SECURITYTYPENAME,
	cs.SECURITYAMOUNT,
	cs.SECURITYEXPIREDATE,
	cs.SECURITYREVIEWDATE,
    cs.CUSTOMER_ID
    c.EFS_CUSTOMER_PK,
    c.CUSTOMER_STATUS,
    c.CUSTOMER_NAME,
    c.GOVERNMENT_ACCOUNT_INDICATOR,
    c.CUSTOMER_ID
from EFS_CREDIT_SECUTIRY cs
join EFS_CUTOMER c on c.CUSTOMER_ID = cs.CUSTOMER_ID
   
