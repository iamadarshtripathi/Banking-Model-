-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(tags=["p4"], materialized= "table" )}}
with loan_transaction as (
     select * from {{ref('raw_loan_transactions')}}
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
-- calculating sanctioned loans

 
loan_info as (

     select  loan_id from raw_loan_transactions where loan_amount is not null group by loan_id
),


-- calculating first,last and next emi dates

loan_emi_info as (
     select  li.loan_id , sum(loan_amount) as loan_amount , max(emi_amount) as emi_amount ,
     case when  min(transaction_date) = max(transaction_date) then DATEADD(month,1, max(transaction_date)) else min(transaction_date) end as First_EMI_DATE
     , case when min(transaction_date) = max(transaction_date) then null else  max(transaction_date) end as Last_EMI_Paid_On
     , case when DATEDIFF(days, max(transaction_date), GETDATE()) > 30 then null else DATEADD(month, 1, max(transaction_date)) end as Next_EMI_Due_Date
     , case when DATEDIFF(days, max(transaction_date), GETDATE()) > 30 then 1 else 0 end as Defaulter
     from {{ref('raw_loan_transactions')}} rlt
     join loan_info li
     on li.loan_id = rlt.loan_id
     group by  li.loan_id 
    
     
),

--calculating months and amount past due date if person is a defaulter

raw_loan_transactions as (
     select * ,
     case when DATEDIFF(month, next_emi_due_date, GETDATE()) > 0 then DATEDIFF(month, next_emi_due_date, GETDATE()) else null end as month_pass_due_date,
     case when DATEDIFF(month, next_emi_due_date, GETDATE()) > 0 then DATEDIFF(month, next_emi_due_date, GETDATE()) * EMI_AMOUNT else 0 end as amount_pass_due_date
     from loan_emi_info
),


elt_columns as (
 select  loan_id as loan_account_id, loan_id , loan_amount ,emi_amount , first_emi_date , last_emi_paid_on ,
 next_emi_due_date , defaulter , month_pass_due_date , amount_pass_due_date ,
    GETDATE() as _elt_inserted ,
    loan_id as _elt_joinkey,
    sha2(concat(ifnull(cast(loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(emi_amount as string), '!@|@!'), '~#~',
    ifnull(cast(loan_amount as string), '!@|@!'), '~#~',
    ifnull(cast(first_emi_date as string), '!@|@!'), '~#~',
    ifnull(cast(last_emi_paid_on as string), '!@|@!'), '~#~',
    ifnull(cast(next_emi_due_date as string), '!@|@!'), '~#~',
    ifnull(cast(defaulter as string), '!@|@!'), '~#~',
    ifnull(cast(month_pass_due_date as string), '!@|@!'), '~#~',
    ifnull(cast(amount_pass_due_date as string), '!@|@!'), '~#~'  ),256)  as _elt_hashkey
    from raw_loan_transactions, elt_logs el
),


final as (
    select * from elt_columns
)

select * from final