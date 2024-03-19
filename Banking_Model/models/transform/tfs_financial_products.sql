with raw_loan_detail as (
    select * from {{source('RAW','RAW_LOAN_DETAILS')}}
),

raw_fixed_deposits as (
    select * from {{source('RAW', 'RAW_FIXED_DEPOSIT')}}
),

raw_temp_account as (
    select * from {{source('RAW', 'RAW_ACCOUNTS')}}
),

financial_products as (
    select 
        rld.loan_id, rld.emi_amount, rld.sanction_date,rld.loan_amount
        , ra.account_number,ra.account_open_date,ra.account_close_date ,rfd.customer_id, rfd.from_date, rfd.interest_rate
        , rfd.issued_date,rfd.to_date
        from raw_temp_account ra   
        join raw_loan_detail rld on ra.account_id=rld.account_number
        join raw_fixed_deposits rfd on ra.customer_id=rfd.customer_id
        
),


final as (
    select * from financial_products

)

select * from final