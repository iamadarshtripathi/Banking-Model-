-- This view is generated to get the details of the fixed deposits

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

fixed_deposit_table as (
select 
customer_id,
fd_id,
certificate_number,
amount,
from_date,
interest_rate,
payment_method,
period,
to_date
from {{ref('raw_fixed_deposit')}}
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

accounts_branch_table as (
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
branch_city,
branch_state,
branch_zip,
branch_code
from customer_account_type_table as catt
join branch_table as bt 
on bt.branch_id=catt.branch_id
),

fixed_deposit_account_table as (
select
account_id,
account_type_id,
account_number,
account_open_date,
account_close_date,
account_status,
branch_id,
fdt.customer_id,
customer_name,
customer_type,
account_type_description,
branch_city,
branch_state,
branch_zip,
branch_code,
fd_id,
certificate_number,
amount,
from_date,
interest_rate,
payment_method,
period,
to_date
from fixed_deposit_table as fdt
join accounts_branch_table as abt
on fdt.customer_id = abt.customer_id
)
select * from fixed_deposit_account_table