with raw_employees as (
    select * from {{source('RAW','RAW_EMPLOYEES')}}
),

raw_loan_details as (
    select * from {{source('RAW','RAW_LOAN_DETAILS')}}
),

loan_details_employees as (
    select rld.loan_id as loan_id ,
    re.employee_id as employee_id ,
    re.tiered_level as tiered_level ,
    re.branch_id as branch_id ,
    rld.loan_amount as loan_amount , 
    rld.loan_type_id as loan_type_id ,
    rld.account_number as account_number 
    from raw_employees re 
    inner join raw_loan_details as rld 
    on re.employee_id = rld.employee_id
),


raw_loan_types as (
    select * from {{ref('tfs_loan_types')}}
),


raw_product_sale as (

    select lde.loan_id as loan_id,
    lde.employee_id as employee_id,
    lde.tiered_level as tiered_level ,
    rlt.loan_type as product_type,
    lde.branch_id as branch_id,
    lde.loan_amount as loan_amount, 
    lde.loan_type_id as loan_type_id,
    lde.account_number as account_number
    from loan_details_employees lde 
    inner join raw_loan_types as rlt 
    on lde.loan_type_id = rlt.loan_type_id

),

raw_commission_rates as (
    select * from {{source('RAW','RAW_COMMISSION_RATES')}}
),


tfs_loan_commission as (
    select  rps.employee_id as employee_id ,
        rcr.commission_target as commission_target,
        rcr.commission_rate as commission_rate,
        rps.loan_amount as loan_amount ,
        rps.loan_type_id as loan_type_id,
        rps.loan_id as loan_id,
        rps.product_type as product_type, 
        rcr.commission_rate_id as commission_rate_id,
        rcr.tiered_level as tiered_level,

       CASE
          WHEN rps.product_type = rcr.product_type AND rps.tiered_level = rcr.tiered_level AND rps.loan_amount >= rcr.commission_target
          THEN rps.loan_amount * rcr.commission_rate
          ELSE 0
        END AS commission_amount    

    from
        raw_product_sale rps inner join raw_commission_rates as rcr
        on rps.product_type = rcr.product_type and rps.tiered_level = rcr.tiered_level
        order by employee_id
),


final as (
    select * from tfs_loan_commission
)

select * from final
