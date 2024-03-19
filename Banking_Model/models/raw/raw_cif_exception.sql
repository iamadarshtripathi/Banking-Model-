{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_cif_exception rce SET rce._ELT_Updated = GETDATE(), rce.exception_id  = st.exception_id , rce.customer_id  = st.customer_id , rce.employee_id  = st.employee_id , rce.open_date = st.open_date , rce.review_date = st.review_date ,
                 rce.doc_status = st.doc_status ,rce.description = st.description ,rce.comments = st.comments 
                 FROM raw.raw_temp_cif_exception st 
                 WHERE rce.exception_id  = st.exception_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_cif_exception rce SET rce._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_cif_exception st 
                 where rce.exception_id  = st.exception_id
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_cif_exception','raw_temp_cif_exception')"
               ] 
            )}}
        
with st_cif_exception as (
     select * from {{ref('raw_temp_cif_exception')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_cif_exception as (
    select exception_id , customer_id , employee_id , open_date , review_date , doc_status, description , comments,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    exception_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(exception_id as string), '!@|@!'), '~#~',
    ifnull(cast(customer_id as string), '!@|@!'), '~#~',
    ifnull(cast(employee_id as string), '!@|@!'), '~#~',
    ifnull(cast(open_date as string), '!@|@!'), '~#~',
    ifnull(cast(review_date as string), '!@|@!'), '~#~',
    ifnull(cast(doc_status as string), '!@|@!'), '~#~',
    ifnull(cast(description as string), '!@|@!'), '~#~',
    ifnull(cast(comments as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_cif_exception, elt_logs el
),  

final as (
    select * from raw_cif_exception
)

select * from final       
