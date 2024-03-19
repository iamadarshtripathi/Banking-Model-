{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_transfer_loan_data rtl SET rtl._ELT_Updated = GETDATE(),
                 rtl.transfer_id = st.transfer_id,
                 rtl.transfer_type = st.transfer_type,
                 rtl.loan_amount = st.loan_amount,
                 rtl.bank_interest_rate = st.bank_interest_rate,
                 rtl.other_interest_rate = st.other_interest_rate
                    FROM raw.raw_temp_transfer_loan_data st 
                    WHERE rtl.transfer_id = st.transfer_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_transfer_loan_data rtl SET rtl._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_transfer_loan_data st 
                    WHERE rtl.transfer_id = st.transfer_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    "call raw.sp_Update_elt_logs('raw_transfer_loan_data','raw_temp_transfer_loan_data')"
               ])
}} 

with st_transfer_loan_data as(
    select * from  {{ ref('raw_temp_transfer_loan_data') }} where metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

raw_transfer_loan_data as (
     select transfer_id , transfer_type , loan_amount , bank_interest_rate , other_interest_rate ,
     current_timestamp() as _elt_inserted ,
    cast('12/30/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    transfer_id as _elt_joinkey,
    sha2(concat(ifnull(cast(transfer_id as string),'!@|@!'),'~#~', 
                ifnull(cast(transfer_type as string),'!@|@!'),'~#~',
                ifnull(cast(loan_amount as string),'!@|@!'),'~#~',
                ifnull(cast(bank_interest_rate as string),'!@|@!'),'~#~',
                ifnull(cast(other_interest_rate as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_transfer_loan_data, elt_logs el
    
),

final as (
    select * from raw_transfer_loan_data
)

select * from final