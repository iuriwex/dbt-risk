{% snapshot ca_collection_cases_snapshot %}

{{
    config(
        target_database='sandbox',
        target_schema='RISK_ENG__W509190',
        unique_key='case_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ source('warehouse','ca_collection_cases')}}

{% endsnapshot %}