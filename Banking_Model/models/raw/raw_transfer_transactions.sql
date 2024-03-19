-- filtered out transfer_transactions from transactions table

{{config(tags=["p4"],post_hook=[
             "UPDATE raw.raw_transfer_transactions r SET r._ELT_Updated = GETDATE() ,
             r.transfer_transaction_id = st.transaction_id,
             r.transaction_id = st.transaction_id,
             r.beneficiary_name = st.beneficiary_name,
             r.originator_name = st.originator_name
             FROM raw.raw_temp_transactions st
             WHERE r.transfer_transaction_id = st.transaction_id
             AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
             "UPDATE raw.raw_transfer_transactions r SET r._ELT_Is_Deleted = 1
             FROM raw.raw_temp_transactions st
             where r.transfer_transaction_id = st.transaction_id
             and st.METADATA$ACTION = 'DELETE' and st.METADATA$ISUPDATE = False"
            ]

)}}

with st_transfer_transactions as (
    select 
        transaction_id as transfer_transaction_id,
        transaction_id,
        beneficiary_name,
        originator_name, 
        metadata$action,
        metadata$isupdate
    from {{ref('raw_temp_transactions')}}
    where transaction_type_id = 14 AND metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

raw_transfer_transactions as(
    select transfer_transaction_id,transaction_id,beneficiary_name,originator_name,
    current_timestamp() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    transfer_transaction_id as _elt_joinkey,
    sha2(concat(ifnull(cast(transfer_transaction_id as string),'!@|@!'),'~#~', 
                ifnull(cast(transaction_id as string),'!@|@!'),'~#~',
                ifnull(cast(beneficiary_name as string),'!@|@!'),'~#~',
                ifnull(cast(originator_name as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_transfer_transactions, elt_logs el
),

final as (
    select * from raw_transfer_transactions
)

select * from final