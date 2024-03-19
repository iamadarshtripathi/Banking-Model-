{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_transaction_types rtt SET rtt._ELT_Updated = GETDATE(),
                 rtt.transaction_type_id = st.transaction_type_id,
                 rtt.transaction_type = st.transaction_type,
                 rtt.transaction_type_description = st.transaction_type_description  
                    FROM raw.raw_temp_transaction_types st 
                    WHERE rtt.transaction_type_id = st.transaction_type_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_transaction_types rtt SET rtt._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_transaction_types st 
                    WHERE rtt.transaction_type_id = st.transaction_type_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    "call raw.sp_Update_elt_logs('raw_transaction_types','raw_temp_transaction_types')"
               ])
}} 

with st_transaction_types as(
    select * from  {{ ref('raw_temp_transaction_types') }} where metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

raw_transaction_types as (
     select transaction_type_id , transaction_type , transaction_type_description , 
     current_timestamp() as _elt_inserted ,
    cast('12/30/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    transaction_type_id as _elt_joinkey,
    sha2(concat(ifnull(cast(transaction_type_id as string),'!@|@!'),'~#~', 
                ifnull(cast(transaction_type as string),'!@|@!'),'~#~',
                ifnull(cast(transaction_type_description as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_transaction_types, elt_logs el
    
),

final as (
    select * from raw_transaction_types
)

select * from final