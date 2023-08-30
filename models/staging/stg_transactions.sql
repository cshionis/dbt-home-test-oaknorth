with source as (
  select 
  distinct transaction_id
, customer_id
, transaction_date
, amount  
   from
  {{ ref('transactions') }}
)
, customers as (
    select
        customer_id
    from {{ ref('stg_customers') }}
)
,
renamed as (
select
-- ids
  transaction_id
, c.customer_id
-- dates
, cast(REGEXP_REPLACE(transaction_date, r'^[^-]+', '2023') as date) as transaction_date
--numeric
, amount
from source s 
-- exclude transactions that have no customer ids in the customers table
join customers c 
on c.customer_id = s.customer_id
-- null customer_ids excluded
where s.customer_id is not null)
select * from renamed