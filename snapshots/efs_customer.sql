{% snapshot customer_snapshot %}

{{
    config(
      target_database='RISK_ANALYTICS',
      target_schema='snapshots',
      unique_key='EFS_CUSTOMER_PK',

      strategy='timestamp',
      updated_at='datetime_updated',
    )
}}

select * from {{ source('FINCRIMES', 'EFS_CUSTOMER') }}

{% endsnapshot %}
