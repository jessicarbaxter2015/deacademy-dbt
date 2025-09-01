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