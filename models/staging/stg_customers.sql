with source as(
  select
  *
  from {{ ref('customers') }}
)
select
-- id
    customer_id
-- string
,   name
-- dates
,   date_of_birth
,   joined_date
from source