with fct_credit_security as (
    select *
    from {{ ref('stg_eltool_credit_security') }}
)
select * from EFS_CREDIT_SECURITY s
