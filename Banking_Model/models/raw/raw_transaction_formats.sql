-- config to make base table transaction_formats not appendable.
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_transaction_formats rtf SET rtf._ELT_Updated = GETDATE() ,
                 rtf.format_id = st.format_id ,
                 rtf.format_type = st.format_type ,
                 rtf.format_description = st.format_description  
                    FROM raw.raw_temp_transaction_formats st 
                    WHERE rtf.format_id = st.format_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_transaction_formats rtf SET rtf._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_transaction_formats st 
                    where rtf.format_id = st.format_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    "call raw.sp_Update_elt_logs('raw_transaction_formats','raw_temp_transaction_formats')" 
               ] 
            )}} 

-- source for accessing staging transaction_formats table data.
with st_transaction_formats as(
    select * from {{ ref('raw_temp_transaction_formats') }} where metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

raw_transaction_formats as (
     select format_id , format_type , format_description , 
     current_timestamp() as _elt_inserted ,
    cast('12/30/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    format_id as _elt_joinkey,
    sha2(concat(ifnull(cast(format_id as string), '!@|@!'),'~#~', 
                ifnull(cast(format_type as string), '!@|@!'),'~#~',
                ifnull(cast(format_description as string), '!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_transaction_formats, elt_logs el
    
),

final as (
    select * from raw_transaction_formats
)

select * from final