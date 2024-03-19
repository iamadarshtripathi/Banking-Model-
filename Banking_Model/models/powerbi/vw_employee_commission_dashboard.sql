-- This is view is created to get the details of employee commission
-- like commission rate, commission generated, product name, employee name and
-- other details that need to create report


with employee_table as (
select 
employee_id,
branch_id,
concat(first_name,' ',last_name) as name,
employee_city,
employee_state,
employee_zip
from {{ref('raw_employees')}}
),

loan_details_table as (
select 
loan_id,
employee_id,
sanction_date,
loan_amount,
loan_type_id
from {{ref('raw_loan_details')}}
),

loan_type_table as (
select
loan_type_id,
loan_type
from {{ref('tfs_loan_types')}}
),

commission_table as (
select 
commission_rate_id,
product_type,
tiered_level,
commission_rate
from {{ref('raw_commission_rates')}}
),

branch_table as (
select 
branch_id,
branch_name,
branch_code
from {{ref('raw_branches')}}
),

loan_detail_type_table as (
select 
ldt.loan_type_id,
loan_type,
loan_id,
employee_id,
sanction_date,
loan_amount
from loan_type_table as ltt
join loan_details_table as ldt
on ltt.loan_type_id=ldt.loan_type_id
),

employee_loan_table as (
select 
et.employee_id,
branch_id,
name,
employee_city,
employee_state,
employee_zip,
loan_type_id,
loan_type,
loan_id,
sanction_date,
loan_amount,
case
    when loan_amount<10000 then 'T1'
    when loan_amount>10000 and loan_amount<30000 then 'T2'
    when loan_amount>30000 and loan_amount<100000 then 'T3'
    when loan_amount>100000 then 'T4'
   end as tiered_level    
from loan_detail_type_table as ldtt
join employee_table as et
on ldtt.employee_id=et.employee_id
),

commission_employee_loan_table as (
select 
commission_rate_id,
ct.product_type,
ct.tiered_level,
commission_rate,
employee_id,
branch_id,
name,
employee_city,
employee_state,
employee_zip,
loan_type_id,
loan_type,
loan_id,
sanction_date,
loan_amount,
cast((loan_amount*commission_rate)/100 as number(10,2))as commission_amount
from commission_table as ct
join employee_loan_table as elt
on ct.product_type=elt.loan_type
and ct.tiered_level=elt.tiered_level
),

branch_commission_employee_table as (
select
commission_rate_id,
product_type,
tiered_level,
commission_rate,
employee_id,
celt.branch_id,
name,
employee_city,
employee_state,
employee_zip,
loan_type_id,
loan_type,
loan_id,
sanction_date,
loan_amount,
commission_amount,
branch_name,
branch_code
from commission_employee_loan_table as celt
join branch_table as bt
on celt.branch_id=bt.branch_id
)
select * from branch_commission_employee_table 

