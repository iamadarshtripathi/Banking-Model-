with raw_account as(
select account_id,
       account_type_id,
       account_open_date,
       account_close_date,
       account_status,
       customer_id,
       branch_id
from {{ref('raw_accounts')}} 
),

raw_account_type as(
select raw_account.*,
       at.account_type,
       at.account_type_description
from raw_account
join {{ref('raw_account_types')}} as at
on raw_account.account_type_id = at.account_type_id
),


raw_customer as(
select raw_account_type.*,
       rc.customer_name,
       rc.customer_city,
       rc.customer_state,
       rc.customer_type,
       rc.monthly_income,
       rc.net_asset_value,
       rc.net_liability
from raw_account_type
join {{ref('raw_customers')}} as rc
on raw_account_type.customer_id = rc.customer_id
),

raw_transaction_accounts as(
select raw_customer.*,
       rta.transaction_account_id,
       rta.current_balance,
       rta.available_balance,
       rta.average_balance_mtd
from raw_customer
join {{ref('tfs_transaction_accounts')}} as rta
on raw_customer.account_id = rta.account_id
)

select * from raw_transaction_accounts