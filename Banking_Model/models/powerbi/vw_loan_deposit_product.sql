-- This view is generated to get the details of the loan deposits

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

loan_details_table as (
select 
loan_id,
sanction_date,
loan_amount,
loan_type_id,
tenure
from {{ref('raw_loan_details')}}
),

loan_transaction_table as (
select 
loan_id,
account_id,
sum(emi_amount)  as paid_amount
from {{ref('raw_loan_transactions')}}
where emi_amount is not null
group by loan_id,account_id
),

loan_details_table as (
select 
loan_id,
sanction_date,
loan_amount,
loan_type_id,
tenure
from {{ref('raw_loan_details')}}
),

loan_accounts_table as (
select 
ltt.loan_id,
account_id,
paid_amount,
sanction_date,
loan_amount,
loan_type_id,
tenure
from loan_transaction_table as ltt
join loan_details_table as ldt
on ltt.loan_id=ldt.loan_id
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

account_loan_product as (
select
lat.account_id,
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
loan_id,
sanction_date,
loan_amount,
loan_type_id,
tenure,
branch_code,
paid_amount
from loan_accounts_table as lat
join accounts_branch_table as abt
on lat.account_id=abt.account_id
)
select * from account_loan_product
