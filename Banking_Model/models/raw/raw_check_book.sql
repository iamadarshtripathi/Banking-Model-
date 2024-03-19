--post hook to get _elt_update , _elt_delete in the table and to call the sp to update raw_elt_logs columns   
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_check_book rcb SET rcb._ELT_Updated = GETDATE() , rcb.ACCOUNT_ID = st.ACCOUNT_ID , rcb.REQUESTED_DATE = st.REQUESTED_DATE
                    , rcb.DATE_ISSUED = st.DATE_ISSUED, rcb.READY_DATE = st.READY_DATE, rcb.DELIVERY_DATE = st.DELIVERY_DATE
                    , rcb.CHECK_NUMBER_FROM = st.CHECK_NUMBER_FROM
                    , rcb.CHECK_NUMBER_TO = st.CHECK_NUMBER_TO    
                 FROM raw.raw_temp_check_book st 
                 WHERE rcb.ACCOUNT_ID = st.ACCOUNT_ID and rcb.DATE_ISSUED = st.DATE_ISSUED
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = 1" ,
                  
                "UPDATE raw.raw_check_book rcb SET rcb._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_check_book st 
                 where rcb.ACCOUNT_ID = st.ACCOUNT_ID and rcb.DATE_ISSUED = st.DATE_ISSUED
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = 0",
                
                "call raw.sp_Update_elt_logs('raw_check_book','raw_temp_check_book')"
               ] 
            )}}


with st_check_book as(
    select * from {{ ref('raw_temp_check_book') }} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

raw_check_book as (
    select ACCOUNT_ID, REQUESTED_DATE, DATE_ISSUED, READY_DATE, DELIVERY_DATE, CHECK_NUMBER_FROM , CHECK_NUMBER_TO,
    GETDATE() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted,
    CONCAT(ACCOUNT_ID,'-',DATE_ISSUED) as _elt_joinkey ,
     sha2(concat(ifnull(cast(ACCOUNT_ID as string), '!@|@!'),'~#~', 
     ifnull(cast(REQUESTED_DATE as string), '!@|@!'),'~#~', 
     ifnull(cast(DATE_ISSUED as string), '!@|@!'),'~#~',
     ifnull(cast(READY_DATE as string), '!@|@!'),'~#~',
     ifnull(cast(DELIVERY_DATE as string), '!@|@!'),'~#~',
     ifnull(cast(CHECK_NUMBER_FROM as string), '!@|@!'),'~#~',
     ifnull(cast(CHECK_NUMBER_TO as string), '!@|@!')), 256) as _elt_hashkey,
     'faker' as _elt_source_system,
     el.elt_run_id as _elt_run_id
     from st_check_book , elt_logs el
),

final as (
    select * from raw_check_book
)

select * from final


