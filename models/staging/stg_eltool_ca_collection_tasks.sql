with source as (
    select *
    from {{ ref('ca_collection_tasks_snapshot')}}
), renamed as (
    select taskid
    -- TODO: listing columns
    from source 
)
select *
from renamed