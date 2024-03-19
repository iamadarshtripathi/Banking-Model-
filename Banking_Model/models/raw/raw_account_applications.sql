-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_account_applications  rc SET rc._elt_updated = GETDATE() ,
                rc.account_application_id = st.account_application_id , rc.application_status = st.application_status ,
                 rc.applied_date = st.applied_date , rc.application_type_id = st.application_type_id ,
                 rc.account_application_state = st.account_application_state
                    FROM raw.raw_temp_account_applications st 
                    WHERE rc.account_application_id = st.account_application_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                    
                "UPDATE raw.raw_account_applications rc SET rc._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_account_applications st 
                    where rc.account_application_id = st.account_application_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                    "call raw.sp_Update_elt_logs('raw_account_applications','raw_temp_account_applications')"
               ] 
            )}}

with st_account_applications as(
    select * from {{ref('raw_temp_account_applications')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
raw_account_applications as (
     select account_application_id , application_status , applied_date , application_type_id , account_application_state,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated,
    false as _elt_is_deleted,
    account_application_id as _elt_joinkey,
    SHA2(CONCAT(IFNULL(CAST(ACCOUNT_APPLICATION_ID AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(APPLICATION_STATUS AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(APPLIED_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(APPLICATION_TYPE_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(ACCOUNT_APPLICATION_STATE AS STRING),'!@|@!'),'~#~'),256)  AS _ELT_HASHKEY,  
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_account_applications, elt_logs el   
    
),

final as (
    select * from raw_account_applications
)

select * from final