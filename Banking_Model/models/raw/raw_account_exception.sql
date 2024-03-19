{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_account_exception rae SET rae._ELT_Updated = GETDATE(), rae.account_exception_id  = st.account_exception_id , rae.account_id  = st.account_id , rae.employee_id  = st.employee_id , rae.exception_date = st.exception_date ,
                 rae.exception_status = st.exception_status , rae.exception_type = st.exception_type , rae.description = st.description 
                 FROM raw.raw_temp_account_exception st 
                 WHERE rae.account_exception_id  = st.account_exception_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_account_exception rae SET rae._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_account_exception st 
                 where rae.account_exception_id  = st.account_exception_id
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_account_exception','raw_temp_account_exception')"
               ] 
            )}}
        
with st_account_exception as (
     select * from {{ref('raw_temp_account_exception')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_account_exception as (
    select account_exception_id , account_id , employee_id , exception_date , exception_status, exception_type , description ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    account_exception_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(account_exception_id as string), '!@|@!'), '~#~',
    ifnull(cast(account_id as string), '!@|@!'), '~#~',
    ifnull(cast(employee_id as string), '!@|@!'), '~#~',
    ifnull(cast(exception_date as string), '!@|@!'), '~#~',
    ifnull(cast(exception_status as string), '!@|@!'), '~#~',
    ifnull(cast(exception_type as string), '!@|@!'), '~#~',
    ifnull(cast(description as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_account_exception, elt_logs el
),  

final as (
    select * from raw_account_exception
)

select * from final       
