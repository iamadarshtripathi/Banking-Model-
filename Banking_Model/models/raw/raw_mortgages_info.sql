{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_mortgages_info rmi SET rmi._ELT_Updated = GETDATE(), rmi.property_id  = st.property_id, rmi.property_address = st.property_address , rmi.property_type = st.property_type ,
                 rmi.property_value = st.property_value ,rmi.year_built = st.year_built ,rmi.property_square_footage = st.property_square_footage ,
                 rmi.property_owner_name = st.property_owner_name ,rmi.property_owner_phonenumber = st.property_owner_phonenumber ,rmi.property_owner_email_id = st.property_owner_email_id,
                 rmi.property_tax_rate = st.property_tax_rate  
                 FROM raw.raw_temp_mortgages_info st 
                 WHERE rmi.property_id = st.property_id 
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_mortgages_info rmi SET rmi._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_mortgages_info st 
                 where rmi.property_id = st.property_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_mortgages_info','raw_temp_mortgages_info')"
               ] 
            )}}
        
with st_mortgages_info as (
     select * from {{ref('raw_temp_mortgages_info')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_mortgages_info as (
    select property_id , property_address , property_type , property_value , year_built , property_square_footage,
    property_owner_name , property_owner_phonenumber , property_owner_email_id , property_tax_rate ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    property_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(property_id as string), '!@|@!'), '~#~',
    ifnull(cast(property_address as string), '!@|@!'), '~#~',
    ifnull(cast(property_type as string), '!@|@!'), '~#~',
    ifnull(cast(property_value as string), '!@|@!'), '~#~',
    ifnull(cast(year_built as string), '!@|@!'), '~#~',
    ifnull(cast(property_square_footage as string), '!@|@!'), '~#~',
    ifnull(cast(property_owner_name as string), '!@|@!'), '~#~',
    ifnull(cast(property_owner_phonenumber as string), '!@|@!'), '~#~',
    ifnull(cast(property_owner_email_id as string), '!@|@!'), '~#~',
    ifnull(cast(property_tax_rate as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_mortgages_info, elt_logs el
),  

final as (
    select * from raw_mortgages_info
)

select * from final       

