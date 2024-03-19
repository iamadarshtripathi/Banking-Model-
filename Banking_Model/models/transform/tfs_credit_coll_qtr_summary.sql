with 
tfs_credit_collection as (
    select LOAN_ID, ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE_DESCRIPTION ,
     TRANSACTION_DATE, CREDITORCOLLECTED, TRANSACTIONAMOUNT  from {{ref('tfs_credit_collection')}}
),

tfs_date as (
    select distinct YEAR_INT, QUARTER_INT, QUARTER_LONG_NAME, QUARTER_SHORT_NAME , START_OF_QUARTER , 
    END_OF_QUARTER ,DATE from {{ref('tfs_date')}}
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
        from tfs_credit_collection tcc join tfs_date  td on tcc.TRANSACTION_DATE = td.DATE
),


sum_balance as(
    select  YEAR_INT , QUARTER_INT , QUARTER_LONG_NAME, QUARTER_SHORT_NAME , 
    sum(debit_credit_balance) as sumbalance , 
    sum(credit_balance) as credit_amount_to_bank , sum(debit_balance) as debit_amount_from_bank ,
    START_OF_QUARTER , END_OF_QUARTER 
    from credit_debit_balance
    group by YEAR_INT, QUARTER_INT, QUARTER_LONG_NAME , QUARTER_SHORT_NAME ,START_OF_QUARTER , END_OF_QUARTER
    order by YEAR_INT , QUARTER_INT , QUARTER_LONG_NAME , QUARTER_SHORT_NAME ,START_OF_QUARTER , END_OF_QUARTER
),


credit_coll_quarter_summary as (
    select a.YEAR_INT , a.QUARTER_INT , a.QUARTER_SHORT_NAME , a.QUARTER_LONG_NAME ,
     a.sumbalance ,  (a.sumbalance + b.credit_amount_to_bank + b.debit_amount_from_bank) as Balance ,
      a.credit_amount_to_bank ,a.debit_amount_from_bank , a.START_OF_QUARTER , 
      a.END_OF_QUARTER from sum_balance a 
    join sum_balance b on (a.YEAR_INT = b.YEAR_INT and a.QUARTER_INT = (b.QUARTER_INT-1)) or
     (a.YEAR_INT = b.YEAR_INT-1 and a.QUARTER_INT=4 and (b.QUARTER_INT=1))
),



final as (
    select * from credit_coll_quarter_summary
)

select * from final
