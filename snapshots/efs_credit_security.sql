{% snapshot efs_creadit_security_snapshot %}

{{
    config(
      target_database='RISK_ANALYTICS',
      target_schema='snapshots',
      unique_key=CREDITLINEID',

      strategy='timestamp',
      updated_at='datetime_updated',
    )
}}

select * from {{ source('FINCRIMES', 'EFS_CREDIT_SECURITY') }}

{% endsnapshot %}
