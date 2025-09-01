{% snapshot patient_snapshot %}
{{ config(
    target_database='DBT_DB_STG',
    target_schema='PUBLIC',
    strategy='check',
    unique_key='PATIENT_ID',
    check_cols=['PATIENT_NAME','PATIENT_CONTACT_NUMBER','PATIENT_EMAIL_ID','PATIENT_ADDRESS']
) }}

with dedup as (
  select
    s.*,
    row_number() over (
      partition by PATIENT_ID
      order by CREATED_AT desc
    ) as rn
  from {{ source('patient','PATIENT_SRC') }} s
)
select *
from dedup
where rn = 1

{% endsnapshot %}