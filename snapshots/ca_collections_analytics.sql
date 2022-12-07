{% snapshot ca_collections_analytics_snapshot %}

{{
    config(
        target_database='sandbox',
        target_schema='RISK_ENG__W509190',
        strategy='timestamp',
        unique_key= 'naf_setid',
        updated_at='updated_at',
    )
}}
select * from {{ source('warehouse','ca_collections_analytics')}}

{% endsnapshot %}