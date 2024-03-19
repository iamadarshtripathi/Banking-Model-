-- This view is generated to show all the application 
-- that are approved and converted to loans


with loan_application_table as (
select 
application_id,
loan_type_id,
loan_amount,
customer_id,
applied_date,
employee_id
from {{ref('raw_loan_applications')}}
),

loan_details_table as (
select 
loan_id,
sanction_date,
application_id
from {{ref('raw_loan_details')}}
),

accounts_table as (
select 
account_id,
branch_id,
account_type_id
from {{ref('raw_accounts')}}
),

loan_transaction_table as (
select 
loan_id,
account_id
from {{ref('raw_loan_transactions')}}
group by loan_id,account_id
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

accounts_type_table as (
select
account_type_id,
account_type_description
from {{ref('raw_account_types')}}
),

loan_types_table as (
select 
loan_type_id,
loan_type
from {{ref('tfs_loan_types')}}
),

branch_accounts_table as (
select 
account_id,
branch_city,
branch_state,
branch_zip,
branch_code,
account_type_id
from branch_table as bt
join accounts_table as at
on bt.branch_id=at.branch_id
),

loan_branch_table as (
select
branch_city,
branch_state,
branch_zip,
branch_code,
loan_id,
account_type_id
from branch_accounts_table as bat
join loan_transaction_table as ltt
on bat.account_id=ltt.account_id
),

application_loan_table as (
select 
lat.application_id,
loan_type_id,
loan_amount,
customer_id,
employee_id,
loan_id,
applied_date,
sanction_date,
datediff(day,applied_date,sanction_date) as days_difference
from loan_application_table as lat
join loan_details_table as ldt
on lat.application_id=ldt.application_id
),

application_branch_table as (
select 
application_id,
loan_type_id,
loan_amount,
customer_id,
employee_id,
alt.loan_id,
applied_date,
sanction_date,
days_difference,
branch_city,
branch_state,
branch_zip,
branch_code,
account_type_id
from application_loan_table as alt
join loan_branch_table as lbt
on alt.loan_id=lbt.loan_id
),

application_type_table as (
select 
ltt.loan_type_id,
loan_type,
application_id,
loan_amount,
customer_id,
employee_id,
loan_id,
applied_date,
sanction_date,
days_difference,
branch_city,
branch_state,
branch_zip,
branch_code,
account_type_id
from loan_types_table as ltt
join application_branch_table as abt
on abt.loan_type_id=ltt.loan_type_id
),

account_type_application_table as (
select 
loan_type_id,
loan_type,
application_id,
loan_amount,
customer_id,
employee_id,
loan_id,
applied_date,
sanction_date,
days_difference,
branch_city,
branch_state,
branch_zip,
branch_code,
account_type_description
from application_type_table as att
join accounts_type_table as atts
on atts.account_type_id=att.account_type_id
)
select * from account_type_application_table