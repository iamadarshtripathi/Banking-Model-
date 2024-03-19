{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_customer_satisfaction rcs SET rcs._ELT_Updated = GETDATE(), rcs.satisfaction_id  = st.satisfaction_id, rcs.application_id = st.application_id , rcs.loan_id = st.loan_id ,
                 rcs.satisfaction_score = st.satisfaction_score ,rcs.feedback = st.feedback ,rcs.date = st.date
                 FROM raw.raw_temp_customer_satisfaction st 
                 WHERE rcs.satisfaction_id = st.satisfaction_id 
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_customer_satisfaction rmi SET rmi._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_customer_satisfaction st 
                 where rmi.satisfaction_id = st.satisfaction_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_customer_satisfaction','raw_temp_customer_satisfaction')"
               ] 
            )}}
        
with st_customer_satisfaction as (
     select * from {{ref('raw_temp_customer_satisfaction')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_customer_satisfaction as (
    select satisfaction_id , application_id , loan_id , satisfaction_score , feedback , date ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    satisfaction_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(satisfaction_id as string), '!@|@!'), '~#~',
    ifnull(cast(application_id as string), '!@|@!'), '~#~',
    ifnull(cast(loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(satisfaction_score as string), '!@|@!'), '~#~',
    ifnull(cast(feedback as string), '!@|@!'), '~#~',
    ifnull(cast(date as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_customer_satisfaction, elt_logs el
),  

final as (
    select * from raw_customer_satisfaction
)

select * from final       
