


with raw_departments as (
    select * from {{ref('raw_departments')}}
),


raw_expense as (
    select * from {{ref('raw_expense')}}
),


department_expense as (
    select re.expense_id , re.department_id, re.expense_date , re.expense_amount , rd.department_name , 
    rd.allocated_budget as department_allocated_budget from raw_departments rd join 
    raw_expense re where re.department_id = rd.department_id 
)
select * from department_expense
