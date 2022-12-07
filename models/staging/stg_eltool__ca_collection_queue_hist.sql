with source as (
    select *
    from {{ ref('ca_collection_queue_hist_snapshot')}}
), renamed as (
    select id
    from source 
)
select *
from renamed