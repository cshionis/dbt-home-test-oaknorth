with source as (
  select * from
  {{ ref('transactions') }}
),
renamed as (
select
-- ids
  transaction_id
, customer_id
-- dates
, cast(REGEXP_REPLACE(transaction_date, r'^[^-]+', '2023') as date) as transaction_date
, date_trunc(cast(REGEXP_REPLACE(transaction_date, r'^[^-]+', '2023') as date),month) as transaction_month
--numeric
, amount
from source
-- null customer_ids excluded
where customer_id is not null)
select * from renamed