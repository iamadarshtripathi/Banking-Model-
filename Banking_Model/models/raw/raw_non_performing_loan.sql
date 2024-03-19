{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_non_performing_loan rnpl SET rnpl._ELT_Updated = GETDATE(), rnpl.non_performing_loan_id  = st.non_performing_loan_id, rnpl.loan_status = st.loan_status , rnpl.last_payment_date = st.last_payment_date ,
                 rnpl.description = st.description ,rnpl.loan_id = st.loan_id
                 FROM raw.raw_temp_non_performing_loan st 
                 WHERE rnpl.non_performing_loan_id  = st.non_performing_loan_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_non_performing_loan rnpl SET rnpl._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_non_performing_loan st 
                 where rnpl.non_performing_loan_id  = st.non_performing_loan_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_non_performing_loan','raw_temp_non_performing_loan')"
               ] 
            )}}
        
with st_non_performing_loan as (
     select * from {{ref('raw_temp_non_performing_loan')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_non_performing_loan as (
    select non_performing_loan_id , loan_status , last_payment_date , description  , loan_id ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    non_performing_loan_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(non_performing_loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(loan_status as string), '!@|@!'), '~#~',
    ifnull(cast(last_payment_date as string), '!@|@!'), '~#~',
    ifnull(cast(description as string), '!@|@!'), '~#~',
    ifnull(cast(loan_id as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_non_performing_loan, elt_logs el
),  

final as (
    select * from raw_non_performing_loan
)

select * from final       
