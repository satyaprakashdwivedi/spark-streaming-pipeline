-- Business rules for orders, kept in dbt and executed inside the streaming pipeline by transform.dbt
select
    order_id,
    customer_id,
    upper(currency)                                    as currency,
    amount,
    {{ gross_amount('amount') }}                       as gross_amount,
    case when amount >= 10000 then 'large'
         when amount >= 1000  then 'medium'
         else 'small' end                              as order_size,
    created_at
from {{ source('stream', 'input') }}
where amount > {{ var('min_amount') }}
