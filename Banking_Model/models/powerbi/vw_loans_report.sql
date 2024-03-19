with raw_loan_type as (
    select 
    loan_type_id,
    interest_rate,
    loan_type
    from {{ref('tfs_loan_types')}}
),

raw_loan_detail as (
    select 
    account_number,
    loan_amount,
    loan_type_id,
    loan_id,
    sanction_date
    from {{ref('raw_loan_details')}}   
),

raw_loan_account as (
    select
    defaulter,
    loan_id
    from {{ref('tfs_loan_accounts')}}
),

raw_branch as (
    select
    branch_city,
    branch_state,
    branch_id
    from {{ref('raw_branches')}}
),

loan_by_product as (
    select 
    rld.loan_id
    ,rld.loan_amount
    ,rld.loan_type_id
    ,rlt.loan_type
    ,rlt.interest_rate
    ,rld.sanction_date
    from raw_loan_type rlt join raw_loan_detail rld on rlt.loan_type_id = rld.loan_type_id
    order by rld.loan_type_id asc
),

defaulters as (
    select 
     lbp.loan_id
    ,lbp.loan_amount
    ,lbp.loan_type_id
    ,lbp.loan_type
    ,lbp.interest_rate
    ,rla.defaulter
    ,lbp.sanction_date
    from raw_loan_account rla join loan_by_product lbp on rla.loan_id = lbp.loan_id
),

loans_page as (
    select
    da.loan_id
    ,da.loan_amount
    ,da.loan_type_id
    ,da.loan_type
    ,da.interest_rate
    ,da.defaulter
    ,rb.branch_state
    ,rb.branch_city
    ,da.sanction_date
    from raw_branch rb join defaulters da on rb.branch_id = da.loan_id
)

select * from loans_page