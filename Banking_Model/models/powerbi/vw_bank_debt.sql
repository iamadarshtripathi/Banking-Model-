-- This is view is created to get the details of debts of bank


with bank_debt as (
    select debt_id , debt_type , principal_amount , interest_rate , issue_date , debt_status 
    from {{ref('raw_bank_debt')}}
)
select * from bank_debt

