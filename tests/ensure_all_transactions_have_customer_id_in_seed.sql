{{ config(severity = 'warn') }}

select
transaction_id
from  {{ ref('transactions') }}
where customer_id is null