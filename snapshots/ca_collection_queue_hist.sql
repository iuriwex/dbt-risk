{% snapshot ca_collection_queue_hist_snapshot %}

{{
    config(
        target_database='sandbox',
        target_schema='RISK_ENG__W509190',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ source('warehouse','ca_collection_queue_hist')}}

{% endsnapshot %}