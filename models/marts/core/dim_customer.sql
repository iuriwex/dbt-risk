with efs_customer as (
    select *
    from {{ ref('stg_eltool__efs_customer') }}
),
state as (
    select *   
    from {{ ref('stg_eltool__state') }}
)
select EFS_CUSTOMER_PK,
    CUSTOMER_STATUS,
    CUSTOMER_NAME,
    GOVERNMENT_ACCOUNT_INDICATOR,
    CUSTOMER_ID
from EFS_CUSTOMER c