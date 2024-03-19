-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config( materialized = "table")}}
with stage_transaction as (
    select * from {{ref('raw_transactions')}}
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

cal_debit_credit as 
(
    select 
        account_id ,
        case 
            when format_id = 1 then transaction_amount 
              else transaction_amount*-1 end as transaction_amount 
        from stage_transaction
) ,

balance  as 
(
    select 
        account_id as transaction_account_id,
        account_id,
        sum(transaction_amount) as current_balance,
        sum(transaction_amount) as available_balance, 
        case
            when sum(transaction_amount) > 50000 then sum(transaction_amount) - 50000
            else 0
        end as uninsured_deposits,
        (DAY(GETDATE()) - 1) as current_day
    from cal_debit_credit
    group by account_id
), 

raw_transaction_account as 
(
    select transaction_account_id , account_id , current_balance , available_balance , uninsured_deposits,
    (current_balance/case when current_day = 0 then 1 else current_day end) as average_balance_mtd 
    from balance
    order by account_id

), 

elt_columns as (
 select  TRANSACTION_ACCOUNT_ID,ACCOUNT_ID,CURRENT_BALANCE,AVAILABLE_BALANCE,UNINSURED_DEPOSITS,AVERAGE_BALANCE_MTD,
    GETDATE() as _elt_inserted ,
    concat(TRANSACTION_ACCOUNT_ID,ACCOUNT_ID) as _elt_joinkey,
    sha2(concat(ifnull(cast(TRANSACTION_ACCOUNT_ID as string), '!@|@!'), '~#~',
    ifnull(cast(ACCOUNT_ID as string), '!@|@!'), '~#~',
    ifnull(cast(CURRENT_BALANCE as string), '!@|@!'), '~#~',
    ifnull(cast(AVAILABLE_BALANCE as string), '!@|@!'), '~#~',
    ifnull(cast(UNINSURED_DEPOSITS as string), '!@|@!'), '~#~',
    ifnull(cast(AVERAGE_BALANCE_MTD as string), '!@|@!'), '~#~'  ),256)  as _elt_hashkey
    from raw_transaction_account
),

final as 
(
    select * from elt_columns
) 


select * from final