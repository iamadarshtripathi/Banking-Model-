
-- This is view is created to get the details of Budgeting

with tfs_date as (
   select date from {{ref('tfs_date')}}
),

fixed_asset as (
   select rce.asset_id , rfa.asset_type , rfa.purchase_cost as asset_purchase_cost,
   rce.expenditure_id , rce.expenditure_date , rce.expenditure_amount
   from {{ref('raw_fixed_asset')}} as rfa join {{ref('raw_capital_expenditure')}} as rce 
   on rfa.asset_id = rce.asset_id 
   
),


bank_debt as (
    select debt_id , debt_type , principal_amount , interest_rate , issue_date , debt_status 
    from {{ref('raw_bank_debt')}}
),

security as (
  select security_id , security_name , security_type , quantity as security_quantity, 
  market_value , purchase_date , purchase_price , purchase_value
  from {{ref('raw_securities')}}
),


department_expense as (
    select re.expense_id , re.department_id , re.expense_date , re.expense_amount , rd.department_name , 
    rd.allocated_budget as department_allocated_budget from {{ref('raw_departments')}} as rd join 
    {{ref('raw_expense')}} as re on re.department_id = rd.department_id and re.expense_id
),


date_join as (
select distinct t1.date , 
case when t1.date = t4.expense_date then t4.expense_id
else null
end as expense_id, 
case when t1.date = t4.expense_date then t4.department_id
else null
end as department_id, 
case when t1.date = t4.expense_date then  t4.expense_amount
else null
end as expense_amount, 
case when t1.date = t4.expense_date then t4.department_name
else null
end as department_name, 
case when t1.date = t4.expense_date then t4.department_allocated_budget
else null
end as department_allocated_budget,
case when t1.date = t2.expenditure_date then t2.asset_id
else null
end as asset_id, 
case when t1.date = t2.expenditure_date then t2.asset_type
else null
end as asset_type, 
case when t1.date = t2.expenditure_date then t2.asset_purchase_cost
else null
end as asset_purchase_cost, 
case when t1.date = t2.expenditure_date then t2.expenditure_id
else null
end as expenditure_id, 
case when t1.date = t2.expenditure_date then t2.expenditure_amount
else null
end as expenditure_amount,case when t1.date = t3.purchase_date then t3.security_id
else null
end as security_id, 
case when t1.date = t3.purchase_date then t3.security_name
else null
end as security_name, 
case when t1.date = t3.purchase_date then t3.security_type
else null
end as security_type, 
case when t1.date = t3.purchase_date then t3.security_quantity
else null
end as security_quantity, 
case when t1.date = t3.purchase_date then t3.market_value
else null
end as market_value, 
case when t1.date = t3.purchase_date then  t3.purchase_price
else null
end as security_purchase_price, 
case when t1.date = t3.purchase_date then t3.purchase_value
else null
end as security_purchase_value,
case when t1.date = t5.issue_date then t5.debt_id
else null
end as debt_id,
case when t1.date = t5.issue_date then t5.debt_type
else null
end as debt_type,
case when t1.date = t5.issue_date then t5.principal_amount
else null
end as principal_amount,
case when t1.date = t5.issue_date then t5.interest_rate
else null
end as interest_rate
from tfs_date t1 left join department_expense t4 on t1.date = t4.expense_date
left join fixed_asset t2 on t1.date = t2.expenditure_date
left join security t3 on t1.date = t3.purchase_date
left join bank_debt t5 on t1.date = t5.issue_date
)


select * from date_join 