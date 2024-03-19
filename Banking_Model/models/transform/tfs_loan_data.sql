with loan_transaction as (
select loan_id,sum(EMI_AMOUNT)as paid_amount,year(transaction_date) as debit_years
from {{ref('raw_loan_transactions')}} group by loan_id,debit_years 
),
loan_details as(
select loan_id,account_number as account_id,year(sanction_date)as credit_year,loan_amount,loan_type_id
from {{ref('raw_loan_details')}}
),
loan_types as (
select loan_type_id,loan_type,interest_rate
from {{ref('tfs_loan_types')}}
),
loan_summary as(
select lt.loan_id, paid_amount, debit_years, account_id, credit_year, loan_amount, lty.loan_type_id,loan_type,interest_rate
from loan_transaction lt join loan_details ld on lt.loan_id=ld.loan_id
join loan_types lty on lty.loan_type_id=ld.loan_type_id
),
accounts as (
select customer_id,account_id,account_type_id
from {{ref('raw_accounts')}}
), account_types as (
select account_type_id,account_type_description
from {{ref('raw_account_types')}}
),
customers as (
select customer_id,customer_type,monthly_income
from {{ref('raw_customers')}}
),
account_summary as (
select acc.customer_id,account_id,acc.account_type_id,account_type_description,customer_type,monthly_income
from accounts acc join account_types at on acc.account_type_id=at.account_type_id
join customers cus on cus.customer_id=acc.customer_id
),
before_projection as(
select customer_id,accs.account_id,account_type_id,account_type_description,
customer_type,monthly_income,loan_id,paid_amount, debit_years, credit_year, loan_amount,
loan_type_id,loan_type,interest_rate
from account_summary accs join loan_summary ls where accs.account_id=ls.account_id
),
after_projection as (
    select rlt.loan_id , sum(rlt.emi_amount) as paid_amount , 
    year(rlt.transaction_date) as payment_year, max(rlt.transaction_date) as last_transaction_date ,
    case 
        when paid_amount is null then   
            case 
                when  DATEDIFF('month', last_transaction_date, max(actual_close_date)) > 12 then  (sum(rlt.loan_amount)*(month(last_transaction_date)/12)) * (12 - month(last_transaction_date))
                when  DATEDIFF('month', last_transaction_date, max(actual_close_date)) < 12 then  (sum(rlt.loan_amount)*(month(last_transaction_date)/12)) * (month(max(actual_close_date))-month(last_transaction_date))
            end
        else paid_amount    
    end as projected_loan_payment        
    from {{source('RAW','RAW_LOAN_TRANSACTIONS')}} as rlt 
    inner join {{ref('tfs_loan_expiration')}} as tfs 
    on rlt.loan_id = tfs.loan_id
    group by rlt.loan_id , payment_year
    order by loan_id desc
),

join_projection as (
    select bp.customer_id , bp.account_id , bp.account_type_id , bp.account_type_description ,
    bp.customer_type , bp.monthly_income,bp.loan_id , ap.paid_amount , ap.payment_year ,
    bp.credit_year as loan_credit_year , bp.loan_amount , bp.loan_type_id , bp.loan_type ,
    bp.interest_rate , ap.last_transaction_date , ap.projected_loan_payment
    from before_projection as bp 
    inner join after_projection as ap
    on bp.loan_id = ap.loan_id 
    and bp.debit_years = ap.payment_year
),

final as (
    select * from join_projection
)

select * from final



