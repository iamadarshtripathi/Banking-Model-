-- This is view is generated to get the deposit overview

with accounts_table as (
select 
account_id,
account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
branch_id,
customer_id
from {{ref('raw_accounts')}}
),

account_type_table as (
select
account_type_id,
account_type_description
from {{ref('raw_account_types')}}
),

customers_table as (
select 
customer_id,
customer_name,
customer_type
from {{ref('raw_customers')}}
),

branch_table as (
select 
branch_id,
branch_city,
branch_state,
branch_zip,
branch_code
from {{ref('raw_branches')}}
),

transaction_accounts_table as (
select
account_id,
current_balance,
available_balance,
average_balance_mtd
from {{ref('tfs_transaction_accounts')}}
),

customer_accounts_table as (
select 
account_id,
account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
branch_id,
ct.customer_id,
customer_name,
customer_type
from customers_table as ct
join accounts_table as at
on at.customer_id=ct.customer_id
),

customer_account_type_table as (
select 
account_id,
cat.account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
branch_id,
customer_id,
customer_name,
customer_type,
account_type_description
from customer_accounts_table as cat
join account_type_table as att
on cat.account_type_id=att.account_type_id
),

transaction_account_table as (
select 
catt.account_id,
account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
branch_id,
customer_id,
customer_name,
customer_type,
account_type_description,
current_balance,
available_balance,
average_balance_mtd
from customer_account_type_table as catt
join transaction_accounts_table as tat
on catt.account_id=tat.account_id

),

branch_transaction_table as (
select
account_id,
account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
bt.branch_id,
customer_id,
customer_name,
customer_type,
account_type_description,
current_balance,
available_balance,
average_balance_mtd,
branch_city,
branch_state,
branch_zip,
branch_code
from branch_table as bt
join transaction_account_table as tct
on bt.branch_id=tct.branch_id
)
select * from branch_transaction_table