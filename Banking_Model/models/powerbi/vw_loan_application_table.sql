
--------------------------------------------------------

-- This view is created to get all the applications that are enrolled in
-- bank 



with loan_application as (
select 
application_id,
loan_amount,
applied_date,
employee_id
from {{ref('raw_loan_applications')}}
)
select * from loan_application