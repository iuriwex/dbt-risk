with source as (
    select *
    from {{ ref('efs_customer_snapshot') }}
), renamed as (
    select EFS_CUSTOMER_PK,
        CUSTOMER_STATUS,
        CUSTOMER_NAME,
        GOVERNMENT_ACCOUNT_INDICATOR,
        CUSTOMER_ID
    from source
)
select *
from renamed
