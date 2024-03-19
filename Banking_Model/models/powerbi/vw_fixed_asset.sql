

with fixed_asset as (
    select * from {{ref('raw_fixed_asset')}}
),


raw_capital_expenditure as (
    select * from {{ref('raw_capital_expenditure')}}
),


joined_table as (
   select rce.asset_id , rfa.asset_type , rfa.purchase_cost as asset_purchase_cost, rfa.current_cost as asset_current_cost,
   rce.expenditure_id , rce.expenditure_date , rce.expenditure_amount
   from fixed_asset rfa join raw_capital_expenditure rce 
   on rfa.asset_id = rce.asset_id 
   
)
select * from joined_table
