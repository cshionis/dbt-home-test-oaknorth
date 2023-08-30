with 
-- create calendar for last 12 months
recursive last_12_months as (
  select date_add(date_trunc(current_date(), month), interval -1 month) as transaction_month
  union all
  select date_add(transaction_month, interval -1 month)
  from last_12_months
  where transaction_month >= date_add(date_trunc(current_date(), month), interval -12 month)
)

-- total amount per customer per month
, pivot_and_aggregate_transactions_to_customer_per_month_grain as (
select
    customer_id
  , date_trunc(transaction_date,month) transaction_month
  , sum(amount) amount_per_month_per_customer
from {{ ref('stg_transactions') }}
group by 1,2
)

, monthly_transaction_amount as (
select
    transaction_month
  , sum(amount_per_month_per_customer) total_amount_per_month
from pivot_and_aggregate_transactions_to_customer_per_month_grain
group by 1
)
 -- identify the top spender customer per month
, top_customer_monthly as (
  select
    transaction_month
  , customer_id
  from pivot_and_aggregate_transactions_to_customer_per_month_grain
qualify row_number() over (partition by transaction_month order by amount_per_month_per_customer desc) =1
)

-- join all metrics to last_12_months spine and calculate percentage change in transaction totals
, final as (select 
    m.transaction_month
  , t.total_amount_per_month
  , lag(t.total_amount_per_month) over (order by m.transaction_month) previous_month_total_amount 
-- relative change formula --> (final value - initial value) / initial value
  , (t.total_amount_per_month - 
            lag(t.total_amount_per_month) over (order by m.transaction_month)) / 
            lag(t.total_amount_per_month) over (order by m.transaction_month) as percentage_change
  , c.customer_id as highest_spending_customer_id  
from last_12_months m
left join monthly_transaction_amount t 
on m.transaction_month = t.transaction_month
left join top_customer_monthly c
on m.transaction_month = c.transaction_month)

select * from final