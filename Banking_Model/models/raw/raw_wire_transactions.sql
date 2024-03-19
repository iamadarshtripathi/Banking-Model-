-- filtered out wire_transactions from transactions

{{config(tags=["p4"],post_hook=[
             "UPDATE raw.raw_wire_transactions r SET r._ELT_Updated = GETDATE() ,
             r.wire_transaction_id = st.transaction_id,
             r.transaction_type_id = st.transaction_type_id,
             r.bank_to_bank_info = st.bank_to_bank_info,
             r.foreign_transaction = st.foreign_transaction,
             r.sender_aba_routing_number = st.sender_aba_routing_number,
             r.receiver_aba_routing_number = st.receiver_aba_routing_number
             FROM raw.raw_temp_transactions st
             WHERE r.wire_transaction_id = st.transaction_id
             AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,

             "UPDATE raw.raw_wire_transactions r SET r._ELT_Is_Deleted = 1
             FROM raw.raw_temp_transactions st
             where r.wire_transaction_id = st.transaction_id
             and st.METADATA$ACTION = 'DELETE' and st.METADATA$ISUPDATE = False"
            ]
)}}

with st_wire_transactions as (
    select          
        transaction_id as wire_transaction_id,
        transaction_id,
        transaction_type_id ,
        bank_to_bank_info,
        foreign_transaction,
        sender_aba_routing_number,
        receiver_aba_routing_number,
        metadata$action,
        metadata$isupdate 
    from {{ref('raw_temp_transactions')}}
    where transaction_type_id = 13 and metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

raw_wire_transactions as(
    select wire_transaction_id,transaction_id,transaction_type_id,bank_to_bank_info,foreign_transaction,sender_aba_routing_number,receiver_aba_routing_number,
    current_timestamp() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    wire_transaction_id as _elt_joinkey,
    sha2(concat(ifnull(cast(wire_transaction_id as string),'!@|@!'),'~#~', 
                ifnull(cast(transaction_type_id as string),'!@|@!'),'~#~',
                ifnull(cast(bank_to_bank_info as string),'!@|@!'),'~#~',
                ifnull(cast(foreign_transaction as string),'!@|@!'),'~#~',
                ifnull(cast(sender_aba_routing_number as string),'!@|@!'),'~#~',
                ifnull(cast(receiver_aba_routing_number as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_wire_transactions, elt_logs el
),

final as (
    select * from raw_wire_transactions
)

select * from final