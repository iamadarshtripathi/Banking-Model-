with raw_loan_transaction as (
  select loan_id, SUM(emi_amount) as amount_paid, SUM(loan_amount) as loan_borrowed_amount, MAX(transaction_date) as last_emi_paid
  from {{source('RAW','RAW_LOAN_TRANSACTIONS')}}
  group by loan_id
  
),
joined_loan_table as (
  select s1.loan_id, s1.amount_paid, s1.loan_borrowed_amount, s1.last_emi_paid, la.defaulter
  from raw_loan_transaction as s1 
  inner join {{ref('tfs_loan_accounts')}} as la on s1.loan_id = la.loan_id
),
loan_closed as (
  select s2.loan_id, s2.defaulter, 
         DATEADD(MONTH, ld.tenure, ld.sanction_date) as expected_close_date,
         case
           when s2.amount_paid = ld.emi_amount * ld.tenure and last_emi_paid = expected_close_date then expected_close_date
           when s2.amount_paid = ld.emi_amount * ld.tenure and last_emi_paid <> expected_close_date then last_emi_paid
           when s2.amount_paid <> ld.emi_amount * ld.tenure and s2.defaulter = 0 then expected_close_date
           when s2.amount_paid <> ld.emi_amount * ld.tenure and s2.defaulter = 1 then null
         END as actual_close_date,
        --  case
        --    when s2.defaulter = 1 then ld.employee_id else null
        --  END as employee_id,
         case
           when s2.amount_paid = ld.emi_amount * ld.tenure and last_emi_paid = expected_close_date then True
           when s2.amount_paid = ld.emi_amount * ld.tenure and last_emi_paid <> expected_close_date then True
           when s2.amount_paid <> ld.emi_amount * ld.tenure and s2.defaulter = 0 then False
           when s2.amount_paid <> ld.emi_amount * ld.tenure and s2.defaulter = 1 then False
         END as valid_loan_closers
  from joined_loan_table as s2
  inner join raw_loan_details as ld on s2.loan_id = ld.loan_id
),
final as (
    select * from loan_closed 
)
select * from final
