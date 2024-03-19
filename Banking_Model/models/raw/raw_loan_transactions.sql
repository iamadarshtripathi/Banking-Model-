-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(tags=["p4"])}}

with st_loan_transactions as (
    select  * from {{ref('raw_transactions')}}
    where transaction_type_id = 8
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

raw_loan_transaction as (
    select
        loan_id,
        account_id,
        transaction_id,
        case
            when format_id = 1 then transaction_amount
            else null
            end as loan_amount,
        
        case
            when format_id <> 1 then transaction_amount
            else null
            end as emi_amount,
        transaction_date     
            
    from
        st_loan_transactions
),
elt_columns as (
 select  LOAN_ID , ACCOUNT_ID ,TRANSACTION_ID,LOAN_AMOUNT,EMI_AMOUNT,TRANSACTION_DATE,
     GETDATE() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    LOAN_ID as _elt_joinkey,
     sha2(concat(ifnull(cast(LOAN_ID as string), '!@|@!'), '~#~',
    ifnull(cast(ACCOUNT_ID as string), '!@|@!'), '~#~',
    ifnull(cast(TRANSACTION_ID as string), '!@|@!'), '~#~',
    ifnull(cast(LOAN_AMOUNT as string), '!@|@!'), '~#~',
    ifnull(cast(EMI_AMOUNT as string), '!@|@!'), '~#~',
    ifnull(cast(TRANSACTION_DATE as string), '!@|@!'), '~#~'  ),256)  as _elt_hashkey
    from raw_loan_transaction, elt_logs el
),

final as (
    select * from elt_columns
)

select * from final