--------------------------------------------------

-- This view is genrated to get the details of application that 
-- are converted to loans and to get days difference of applied date 
-- to sanction date




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
)
select * from application_loan_table


