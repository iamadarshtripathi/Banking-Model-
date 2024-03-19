-- This is view is created to get the details of all loans and its data



with total_loan as (
   select * from {{ref('raw_loan_details')}}
),

non_performing_loan as (
   select * from {{ref('raw_non_performing_loan')}}
),

loan_income as (
     select loan_id , sum(emi_amount) as loan_emi_amount
     from {{ref('raw_loan_transactions')}}
     where  emi_amount is not null 
     group by loan_id ,transaction_date
),

non_performing_loan2 as (
   select tl.loan_id , tl.sanction_date ,
   case
         when tl.loan_id = npl.loan_id and npl.non_performing_loan_id is not null then npl.non_performing_loan_id
         else null
         end as non_performing_loan_id,
          case
         when tl.loan_id = npl.loan_id and npl.loan_status is not null then npl.loan_status
         else null
         end as loan_status,
          tl.loan_amount  
   from total_loan tl
   left join non_performing_loan npl 
   on tl.loan_id = npl.loan_id

),

loan_emi as (
select  npl.* , 
          case
         when npl.loan_id = li.loan_id and li.loan_emi_amount is not null then li.loan_emi_amount
         else null
         end as loan_emi_amount 
from non_performing_loan2 npl
left join loan_income li
on npl.loan_id = li.loan_id
)

select * from loan_emi