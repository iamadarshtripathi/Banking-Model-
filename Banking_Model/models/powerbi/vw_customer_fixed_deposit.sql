with raw_customer as(
select rc.customer_id,
       rc.customer_name,
       rc.customer_city,
       rc.customer_state,
       rc.customer_type,
       rc.monthly_income,
       rc.net_asset_value,
       rc.net_liability
from {{ref('raw_customers')}} as rc
),

raw_fixed_deposits as(
select raw_customer.* ,
       rfd.amount,
       rfd.from_date,
       rfd.interest_rate,
       rfd.period,
       rfd.issued_date,
       rfd.to_date
from raw_customer
join {{ref('raw_fixed_deposit')}} as rfd
on raw_customer.customer_id = rfd.customer_id
),

raw_account as(
select raw_fixed_deposits.*,
       ra.account_id,
       ra.account_type_id,
       ra.account_open_date,
       ra.account_close_date,
       ra.account_status,
       ra.branch_id
from raw_fixed_deposits
left join {{ref('raw_accounts')}} as ra
on raw_fixed_deposits.customer_id = ra.customer_id
),

raw_account_type as(
select raw_account.*,
       at.account_type,
       at.account_type_description
from raw_account
left join {{ref('raw_account_types')}} as at
on raw_account.account_type_id = at.account_type_id
),

raw_transaction_accounts as(
select raw_account_type.*,
       rta.transaction_account_id,
       rta.current_balance,
       rta.available_balance,
       rta.average_balance_mtd
from raw_account_type
left join {{ref('tfs_transaction_accounts')}} as rta
on raw_account_type.account_id = rta.account_id
)

select * from raw_transaction_accounts