with transactions as (
select
*
from {{ ref('stg_transactions') }}
)
 -- identify transaction interval
, transactions_prep as (
  select
    t.transaction_id
  , t.customer_id
  , t.transaction_date
  , t.transaction_month
  , t.amount
  , date_diff(t.transaction_date, lag(t.transaction_date) over (partition by t.customer_id order by t.transaction_date), day) transaction_interval_in_days
  from transactions t 
)
-- aggregate on transactions on the customer grain
, pivot_and_aggregate_payments_to_customer_grain as (
  select
  customer_id
  , count(*) as transactions_to_date
  , round(avg(transaction_interval_in_days),2) as avg_transaction_interval_in_days
  , min(transaction_date) as first_transaction_date
  , min(transaction_month) as first_transaction_month
  , max(transaction_month) as last_transaction_month
  , date_diff(max(transaction_month),min(transaction_month),month)+1 number_of_months_active
  , sum(amount) as total_amount
  from transactions_prep
  group by 1
)
,  final as (
  select
    customer_id
  , first_transaction_date
  , transactions_to_date
  , round(total_amount / number_of_months_active,2) as avg_monthly_spending
  , avg_transaction_interval_in_days
  from pivot_and_aggregate_payments_to_customer_grain
)
select
*
from final
