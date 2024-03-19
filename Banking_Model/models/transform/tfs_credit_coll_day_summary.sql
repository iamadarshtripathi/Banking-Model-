with 
tfs_credit_collection as (
    select LOAN_ID, ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE_DESCRIPTION
    , TRANSACTION_DATE, CREDITORCOLLECTED, TRANSACTIONAMOUNT  from {{ref('tfs_credit_collection')}}
),

tfs_date as (
    select distinct YEAR_INT, month_int, month_short_name, month_long_name , start_of_month , end_of_month ,
    QUARTER_INT, QUARTER_LONG_NAME, QUARTER_SHORT_NAME , START_OF_QUARTER , 
    END_OF_QUARTER , DATE from {{ref('tfs_date')}}
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


day_summary as (
    select  LOAN_ID, ACCOUNT_ID,DATE , YEAR_INT, month_int, month_short_name,
     month_long_name , start_of_month , end_of_month ,
    QUARTER_INT, QUARTER_LONG_NAME, QUARTER_SHORT_NAME , START_OF_QUARTER , 
    END_OF_QUARTER ,CREDITORCOLLECTED,(
    select  sum(credit_balance +  debit_balance)
    from credit_debit_balance t2
    where t2.DATE <= t1.DATE
    ) as balance, 
    credit_balance as credit_amount_to_bank , debit_balance as debit_amount_from_bank
    from credit_debit_balance t1
    order by DATE 
),

final as (
    select * from day_summary
)

select * from final

