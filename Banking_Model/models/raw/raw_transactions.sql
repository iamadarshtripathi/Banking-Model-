-- source for accessing staging transactions table data.
{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_transactions rt SET rt._ELT_Updated = CURRENT_TIMESTAMP() ,rt.transaction_id = st.transaction_id , rt.account_id = st.account_id ,
                 rt.format_id = st.format_id , rt.transaction_type_id = st.transaction_type_id ,
                 rt.source = st.source , rt.filename = st.filename , rt.sender = st.sender, rt.receiver = st.receiver , 
                 rt.transaction_date = st.transaction_date , rt.process_date = st.process_date ,rt.process_status = st.process_status, rt.created_by = st.created_by ,
                 rt.created_date = st.created_date , rt.updated_date = st.updated_date , rt.updated_by = st.updated_by ,
                 rt.currency_code = st.currency_code , rt.check_number = st.check_number , rt.transaction_amount = st.transaction_amount , rt.bank_to_bank_info = st.bank_to_bank_info , rt.foreign_transaction = st.foreign_transaction ,
                 rt.sender_aba_routing_number = st.sender_aba_routing_number , rt.receiver_aba_routing_number = st.receiver_aba_routing_number , rt.beneficiary_name = st.beneficiary_name , rt.originator_name = st.originator_name ,
                 rt.ach_addenda = st.ach_addenda , rt.loan_id = st.loan_id , rt.loan_type_id = st.loan_type_id
                    FROM raw.raw_temp_transactions st 
                    WHERE rt.transaction_id = st.transaction_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_transactions rt SET rt._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_transactions st 
                    where rt.transaction_id = st.transaction_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    "call raw.sp_Update_elt_logs('raw_transactions','raw_temp_transactions')"
               ] 
            )}}
with st_transactions as(
    select * from {{ ref('raw_temp_transactions') }} where metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

raw_transactions as (
     select transaction_id , account_id , format_id , transaction_type_id , source , filename ,sender , receiver,
    transaction_date, process_date , process_status , created_by , created_date , updated_date , updated_by ,
    currency_code , check_number , transaction_amount , bank_to_bank_info , foreign_transaction , sender_aba_routing_number,
    receiver_aba_routing_number , beneficiary_name , originator_name , ach_addenda , loan_id, loan_type_id,
    current_timestamp() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted,
    transaction_id as _elt_joinkey,
    sha2(concat(ifnull(cast(transaction_id as string), '!@|@!'), '~#~', 
                ifnull(cast(account_id as string), '!@|@!'), '~#~', 
                ifnull(cast(format_id as string), '!@|@!'), '~#~',
                ifnull(cast(transaction_type_id as string), '!@|@!'), '~#~',
                ifnull(cast(source as string), '!@|@!'), '~#~',
                ifnull(cast(filename as string), '!@|@!'), '~#~',
                ifnull(cast(sender as string), '!@|@!'), '~#~',
                ifnull(cast(receiver as string), '!@|@!'), '~#~',
                ifnull(cast(transaction_date as string), '!@|@!'), '~#~',
                ifnull(cast(process_date as string), '!@|@!'), '~#~',
                ifnull(cast(process_status as string), '!@|@!'), '~#~',
                ifnull(cast(created_by as string), '!@|@!'), '~#~',
                ifnull(cast(created_date as string), '!@|@!'), '~#~',
                ifnull(cast(updated_date as string), '!@|@!'), '~#~',
                ifnull(cast(updated_by as string), '!@|@!'), '~#~',
                ifnull(cast(currency_code as string), '!@|@!'), '~#~',
                ifnull(cast(check_number as string), '!@|@!'), '~#~',
                ifnull(cast(transaction_amount as string), '!@|@!'), '~#~',
                ifnull(cast(bank_to_bank_info as string), '!@|@!'), '~#~',
                ifnull(cast(foreign_transaction as string), '!@|@!'), '~#~',
                ifnull(cast(sender_aba_routing_number as string), '!@|@!'), '~#~',
                ifnull(cast(receiver_aba_routing_number as string), '!@|@!'), '~#~',
                ifnull(cast(beneficiary_name as string), '!@|@!'), '~#~',
                ifnull(cast(originator_name as string), '!@|@!'), '~#~',
                ifnull(cast(ach_addenda as string), '!@|@!'), '~#~',
                ifnull(cast(loan_id as string), '!@|@!'), '~#~',
                ifnull(cast(loan_type_id as string), '!@|@!'), '~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_transactions, elt_logs el
       
),

final as (
    select * from raw_transactions )

select * from final