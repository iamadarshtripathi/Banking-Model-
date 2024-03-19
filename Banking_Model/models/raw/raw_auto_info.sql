{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_auto_info rai SET rai._ELT_Updated = GETDATE(), rai.auto_id  = st.auto_id, rai.brand = st.brand , rai.model = st.model ,
                 rai.price = st.price 
                 FROM raw.raw_temp_auto_info st 
                 WHERE rai.auto_id = st.auto_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_auto_info rai SET rai._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_auto_info st 
                 where rai.auto_id = st.auto_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_auto_info','raw_temp_auto_info')"
               ] 
            )}}
        
with st_auto_info as (
     select * from {{ref('raw_temp_auto_info')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_auto_info as (
    select auto_id , brand , model , price ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    auto_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(auto_id as string), '!@|@!'), '~#~',
    ifnull(cast(brand as string), '!@|@!'), '~#~',
    ifnull(cast(model as string), '!@|@!'), '~#~',
    ifnull(cast(price as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_auto_info, elt_logs el
),  

final as (
    select * from raw_auto_info
)

select * from final       

