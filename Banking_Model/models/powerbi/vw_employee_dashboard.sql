-- This view is generated to get all the employees details with 
-- branches they are working, revenue they have generated and many more 


with branch_table as (
select 
branch_id,
branch_head_id,
branch_name,
branch_city,
branch_state,
branch_zip,
branch_code
from {{ref('raw_branches')}}
),

employee_table as (
select 
branch_id,
employee_id,
employee_city,
employee_state,
employee_zip,
first_name,
last_name,
grade
from {{ref('raw_employees')}}
),

loan_detail_table as (
select
loan_id,
employee_id,
sanction_date,
loan_amount,
emi_amount,
tenure,
loan_type_id
from {{ref('raw_loan_details')}}
),

loan_type_table as (
select 
loan_type_id,
loan_type
from {{ref('tfs_loan_types')}}
),

loan_account_table as (
select
loan_id,
defaulter
from {{ref('tfs_loan_accounts')}}
),

branch_employee_table as (
select 
bt.branch_id,
bt.branch_head_id,
bt.branch_name,
bt.branch_city,
bt.branch_state,
bt.branch_zip,
bt.branch_code,
employee_id,
employee_city,
employee_state,
employee_zip,
concat(et.first_name,' ',et.last_name) as employee_name,
et.grade
from branch_table as bt
join employee_table as et
on bt.branch_id=et.branch_id
)
,

employee_loan_table as (
select
branch_id,
branch_head_id,
branch_name,
branch_city,
branch_state,
branch_zip,
branch_code,
employee_city,
employee_state,
employee_zip,
employee_name,
grade,
ldt.employee_id,
loan_id,
sanction_date,
(emi_amount*tenure)-loan_amount as revenue,
loan_type_id
from branch_employee_table as bet
join loan_detail_table as ldt
on bet.employee_id=ldt.employee_id
)
,
employee_loan_type_table as (
select 
ltt.loan_type_id,
loan_type,
branch_id,
branch_head_id,
branch_name,
branch_city,
branch_state,
branch_zip,
branch_code,
employee_city,
employee_state,
employee_zip,
employee_name,
grade,
employee_id,
loan_id,
sanction_date,
revenue
from loan_type_table as ltt
join employee_loan_table as elt
on ltt.loan_type_id = elt.loan_type_id
),

employee_loan_account_table as (
select
eltt.loan_id,
defaulter,
loan_type_id,
loan_type,
branch_id,
branch_head_id,
branch_name,
branch_city,
branch_state,
branch_zip,
branch_code,
employee_city,
employee_state,
employee_zip,
employee_name,
grade,
employee_id,
sanction_date,
revenue
from employee_loan_type_table as eltt
join loan_account_table as lat
on eltt.loan_id=lat.loan_id
)
select * from employee_loan_account_table
