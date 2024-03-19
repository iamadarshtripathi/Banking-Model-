with tfs_credit_collection as (
    select LOAN_ID, ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE_DESCRIPTION
    , TRANSACTION_DATE, CREDITORCOLLECTED, TRANSACTIONAMOUNT  from {{ref('tfs_credit_collection')}}
),

tfs_date as (
    select distinct YEAR_INT,START_OF_YEAR, END_OF_YEAR, DATE from {{ref('tfs_date')}}
),


credit_debit_balance as (
      select tcc.* ,
        case 
        when CREDITORCOLLECTED = 'Collected' then transactionamount 
        else transactionamount * -1 end as debit_credit_balance,
        case 
        when CREDITORCOLLECTED = 'Collected' then transactionamount 
        else 0
        end as credit_balance,
        case 
        when CREDITORCOLLECTED = 'Credit' then transactionamount * -1
        else 0
        end as debit_balance,
        td.*
        from tfs_credit_collection tcc join tfs_date td on tcc.TRANSACTION_DATE = td.DATE

),


credit_coll_year_summary as (
    select  YEAR_INT as year, sum(debit_credit_balance) as balance , 
    sum(credit_balance) as credit_amount_to_bank , sum(debit_balance) as debit_amount_from_bank ,
    START_OF_YEAR , END_OF_YEAR
    from credit_debit_balance
    group by YEAR_INT , START_OF_YEAR , END_OF_YEAR

),

final as (
    select * from credit_coll_year_summary
)

select * from final

