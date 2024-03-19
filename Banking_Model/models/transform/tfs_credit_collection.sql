with raw_loan_details as (
    select * from {{source('RAW', 'RAW_LOAN_DETAILS')}}
),

raw_loan_transactions as (
    select * from {{source('RAW', 'RAW_LOAN_TRANSACTIONS')}}
),

raw_accounts as (
    select * from {{source('RAW', 'RAW_ACCOUNTS')}} 
),

raw_customers as (
    select * from {{source('RAW', 'RAW_CUSTOMERS')}}
),

raw_account_types as (
    select * from {{source('RAW', 'RAW_ACCOUNT_TYPES')}}
),

raw_fixed_deposit as (
    select * from {{source('RAW', 'RAW_FIXED_DEPOSIT')}}
),
raw_branches as (
    select * from {{source('RAW', 'RAW_BRANCHES')}}
),

loandata as (
    select 
        rlt.loan_id, ra.Account_id, rc.customer_id, rc.customer_name, rc.customer_address, rc.customer_address_line2
        , rc.customer_city, rc.customer_state
        , rc.customer_zip, rc.customer_phone, rc.customer_type, rc.email
        , rat.account_type_description, ra.account_number, ra.account_open_date, ra.account_status
        , rlt.transaction_id, rlt.loan_amount, rlt.emi_amount, rlt.transaction_date
        , case when rlt.loan_amount is not null then 'Credit' else 'Collected' end CreditOrCollected
        , case when rlt.loan_amount is not null then rlt.loan_amount else rlt.emi_amount end TransactionAmount
        , rc.monthly_income, rc.net_asset_value, rc.net_liability
    from 
    raw_loan_details rld
    JOIN raw_loan_transactions rlt on rld.loan_id = rlt.loan_id
    JOIN raw_accounts ra on ra.account_id = rld.account_number
    JOIN raw_customers rc on rc.customer_id = ra.customer_id
    JOIN raw_account_types rat on rat.account_type_id = ra.account_type_id
),

fddata as (
    select 
        rfs.fd_id, null, rc.customer_id, rc.customer_name, rc.customer_address, rc.customer_address_line2
        , rc.customer_city, rc.customer_state
        , rc.customer_zip, rc.customer_phone, rc.customer_type, rc.email
        , null, null, null, null
        , rfs.receipt_number, rfs.amount, null, rfs.issued_Date
        , 'Collected' CreditOrCollected
        , rfs.amount TransactionAmount
        , rc.monthly_income, rc.net_asset_value, rc.net_liability
    from raw.raw_fixed_deposit rfs
    JOIN raw.raw_customers rc on rfs.customer_id = rc.customer_id
    JOIN raw.raw_accounts ra on ra.customer_id = rc.customer_id
),

final as (
    select * from loandata
    UNION
    select * from fddata

)

select * from final