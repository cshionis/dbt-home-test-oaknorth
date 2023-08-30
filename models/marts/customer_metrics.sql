with customers as (
select
*
from {{ ref('customers') }}
)
, int_transactions as (
select
*
from {{ ref('int_transactions_pivoted_to_customers') }}
)
select
  c.customer_id
, c.name
, c.date_of_birth
, coalesce(c.joined_date,t.first_transaction_date) as joined_date
, t.first_transaction_date
, t.most_recent_transaction_date
, t.transactions_to_date
, t.avg_monthly_spending
, t.avg_transaction_interval_in_days
from customers c
left join int_transactions t
on c.customer_id = t.customer_id