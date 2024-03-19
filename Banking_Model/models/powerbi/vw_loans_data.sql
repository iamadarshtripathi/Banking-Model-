with raw_loans_detail as(
    select loan_id,
           account_number,
           sanction_date,
           loan_amount,
           loan_type_id,
           tenure
    from {{ref('raw_loan_details')}}
)

select * from raw_loans_detail