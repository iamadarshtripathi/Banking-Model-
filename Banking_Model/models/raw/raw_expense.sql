{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_expense re SET re._ELT_Updated = GETDATE(), re.expense_id  = st.expense_id , re.expense_date  = st.expense_date , re.expense_amount = st.expense_amount , 
                re.description = st.description , re.department_id = st.department_id 
                 FROM raw.raw_temp_expense st 
                 WHERE re.expense_id  = st.expense_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_expense re SET re._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_expense st 
                 where re.expense_id  = st.expense_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_expense','raw_temp_expense')"
               ] 
            )}}
        
with st_expense as (
     select * from {{ref('raw_temp_expense')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_expense as (
    select expense_id , expense_date , expense_amount , description , department_id ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    expense_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(expense_id as string), '!@|@!'), '~#~',
    ifnull(cast(expense_date as string), '!@|@!'), '~#~',
    ifnull(cast(expense_amount as string), '!@|@!'), '~#~',
    ifnull(cast(description as string), '!@|@!'), '~#~',
    ifnull(cast(department_id as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_expense, elt_logs el
),  

final as (
    select * from raw_expense
)

select * from final  