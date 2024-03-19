{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_map_auto_loan rai SET rai._ELT_Updated = GETDATE(), rai.loan_id  = st.loan_id, rai.auto_id = st.auto_id  
                 FROM raw.raw_temp_map_auto_loan st 
                 WHERE rai.loan_id = st.loan_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_map_auto_loan rai SET rai._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_map_auto_loan st 
                 where rai.loan_id = st.loan_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_map_auto_loan','raw_temp_map_auto_loan')"
               ] 
            )}}
        
with st_auto_info as (
     select * from {{ref('raw_temp_map_auto_loan')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_auto_info as (
    select loan_id , auto_id ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    concat(loan_id,'-',auto_id) as _elt_joinkey ,
    sha2(concat(ifnull(cast(loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(auto_id as string), '!@|@!'), '~#~',256)) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_auto_info, elt_logs el
),  

final as (
    select * from raw_auto_info
)

select * from final       

