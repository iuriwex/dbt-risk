with source as (
    select *
    from {{ ref('ca_driver_snapshot')}}
), renamed as (
    select id
    from source 
)
select *
from renamed