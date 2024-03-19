-- This view is created to generate daily deposit

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

transactions_table as (
select 
transaction_id,
transaction_type_id,
account_id,
format_id,
source,
transaction_date,
process_status,
transaction_amount
from {{ref('raw_transactions')}}
where format_id=1 
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

transaction_accounts_table as (
select 
tt.account_id,
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
branch_city,
branch_state,
branch_zip,
branch_code,
transaction_id,
transaction_type_id,
source,
transaction_date,
process_status,
transaction_amount
from accounts_branch_table as abt 
join transactions_table as tt
on abt.account_id=tt.account_id

)

select * from transaction_accounts_table
