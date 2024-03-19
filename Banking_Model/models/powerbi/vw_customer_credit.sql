with raw_customer as(
select customer_id,
       customer_name,
       customer_city,
       customer_state,
       customer_type,
       created_date,
       updated_date,
       monthly_income,
       net_asset_value,
       net_liability
from {{ref('raw_customers')}}
),

raw_card_detail as(
select raw_customer.*,
       rcd.card_id,
       rcd.card_type,
       rcd.card_sub_type,
       rcd.issued_date,
       rcd.valid_date
from raw_customer
join {{ref('raw_card_details')}} as rcd
on raw_customer.customer_id = rcd.customer_id
)
select * from raw_card_detail