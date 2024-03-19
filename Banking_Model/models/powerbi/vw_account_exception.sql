-- This is view is created to get the details of account exceptions



with customers_accounts as (
    select ra.account_id , rc.customer_id , rc.customer_name 
    from {{ref('raw_customers')}} as rc join {{ref('raw_accounts')}} as ra 
    where ra.customer_id = rc.customer_id
) ,

account_exception as (
    select rae.ACCOUNT_EXCEPTION_ID , rae.account_id ,ca.customer_id ,  ca.customer_name , rae.EMPLOYEE_ID, rae.EXCEPTION_DATE  , rae.EXCEPTION_STATUS , rae.EXCEPTION_TYPE ,  
    rae.DESCRIPTION   from
    {{ref('raw_account_exception')}} as rae join customers_accounts ca 
    where rae.account_id = ca.account_id
),

employee_name as(
    select ae.* , re.first_name as employee_name from 
    {{ref('raw_employees')}} as re join account_exception ae
    where re.EMPLOYEE_ID = ae.EMPLOYEE_ID
)

select * from employee_name

