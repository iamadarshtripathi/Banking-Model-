-- This is the view that show all the application that are 
-- applied in the bank

with application_table as (
select 
application_id,
loan_type_id,
customer_id,
account_id,
applied_date
from {{ref('raw_loan_applications')}}
),

accounts_table as (
select 
account_id,
account_type_id,
account_status,
branch_id
from {{ref('raw_accounts')}}
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

loan_types_table as (
select 
loan_type_id,
loan_type
from {{ref('tfs_loan_types')}}
),

accounts_type_table as (
select
account_type_id,
account_type_description
from {{ref('raw_account_types')}}
),

application_account_table as (
select 
application_id,
loan_type_id,
customer_id,
at.account_id,
applied_date,
att.account_type_id,
account_status,
branch_id,
account_type_description
from application_table as at
join accounts_table as acct on at.account_id=acct.account_id
join accounts_type_table as att on acct.account_type_id=att.account_type_id
),

branch_application_table as (
select 
application_id,
aat.loan_type_id,
customer_id,
account_id,
applied_date,
account_type_id,
account_status,
bt.branch_id,
account_type_description,
branch_city,
branch_state,
branch_zip,
branch_code,
loan_type
from application_account_table as aat
join branch_table as bt on aat.branch_id=bt.branch_id
join loan_types_table as ltt on ltt.loan_type_id=aat.loan_type_id
)
select * from branch_application_table