{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_refinancing_loan rrl SET rrl._ELT_Updated = GETDATE(), rrl.refinancing_id  = st.refinancing_id, rrl.loan_id = st.loan_id , rrl.refinancing_type = st.refinancing_type ,
                 rrl.refinancing_loan_amount = st.refinancing_loan_amount ,rrl.originating_date = st.originating_date ,rrl.closing_date = st.closing_date ,
                 rrl.appraised_value = st.appraised_value 
                 FROM raw.raw_temp_refinancing_loan st 
                 WHERE rrl.refinancing_id = st.refinancing_id 
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_refinancing_loan rmi SET rmi._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_refinancing_loan st 
                 where rmi.refinancing_id = st.refinancing_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_refinancing_loan','raw_temp_refinancing_loan')"
               ] 
            )}}
        
with st_refinancing_loan as (
     select * from {{ref('raw_temp_refinancing_loan')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_refinancing_loan as (
    select refinancing_id , loan_id , refinancing_type , refinancing_loan_amount , originating_date , closing_date,
    appraised_value ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    refinancing_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(refinancing_id as string), '!@|@!'), '~#~',
    ifnull(cast(loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(refinancing_type as string), '!@|@!'), '~#~',
    ifnull(cast(refinancing_loan_amount as string), '!@|@!'), '~#~',
    ifnull(cast(originating_date as string), '!@|@!'), '~#~',
    ifnull(cast(closing_date as string), '!@|@!'), '~#~',
    ifnull(cast(appraised_value as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_refinancing_loan, elt_logs el
),  

final as (
    select * from raw_refinancing_loan
)

select * from final       
