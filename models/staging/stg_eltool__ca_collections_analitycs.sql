with source as (
    select *
    from {{ ref('ca_collections_analytics_snapshot')}}
), renamed as (
    select id
    from source 
)
select *
from renamed