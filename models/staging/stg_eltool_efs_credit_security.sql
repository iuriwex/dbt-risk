with source as (
    select *
    from {{ ref('efs_credit_security_snapshot') }}
), renamed as (
    select CREDITLINEID,
        SECURITYTYPESEQUENCENUMBER,
        CUSTOMERID,
        SECURITYTYPECODE,
        SECURITYTYPENAME,
        SECURITYAMOUNT,
        SECURITYEXPIREDATE,
        SECURITYREVIEWDATE
    from source
)
select *
from renamed
