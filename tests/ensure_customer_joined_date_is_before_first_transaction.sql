{{ config(severity = 'warn') }}

select
customer_id 
from {{ ref('customer_metrics') }}
where joined_date > first_transaction_date