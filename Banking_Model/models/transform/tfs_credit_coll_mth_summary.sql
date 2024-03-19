with 
tfs_credit_collection as (
    select LOAN_ID, ACCOUNT_ID, CUSTOMER_ID, ACCOUNT_TYPE_DESCRIPTION ,
     TRANSACTION_DATE, CREDITORCOLLECTED, TRANSACTIONAMOUNT  from {{ref('tfs_credit_collection')}}
),

tfs_date as (
    select distinct YEAR_INT, month_int, month_short_name, month_long_name , start_of_month , end_of_month ,
     DATE from {{ref('tfs_date')}}
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


sum_balance as (
    select  YEAR_INT, month_int, month_short_name, month_long_name , 
    sum(debit_credit_balance) as sumbalance , 
    sum(credit_balance) as credit_amount_to_bank , sum(debit_balance) as debit_amount_from_bank ,
    start_of_month , end_of_month
    from credit_debit_balance
    group by YEAR_INT,  month_int, month_short_name, month_long_name , start_of_month , end_of_month
    order by YEAR_INT ,  month_int, month_short_name, month_long_name , start_of_month , end_of_month
),


credit_coll_month_summary as (
    select YEAR_INT , month_int , month_short_name , month_long_name , 
    (   select sum(credit_amount_to_bank + debit_amount_from_bank) 
    from sum_balance t2
    where t2.YEAR_INT<=t1.YEAR_INT and  t2.month_int<=t1.month_int)  as Balance,
    credit_amount_to_bank ,
    debit_amount_from_bank  , 
    start_of_month , end_of_month from sum_balance t1
    order by YEAR_INT , month_int
),


final as (
    select * from credit_coll_month_summary
)

select * from final

