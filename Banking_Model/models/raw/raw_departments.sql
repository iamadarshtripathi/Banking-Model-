{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_departments rd SET rd._ELT_Updated = GETDATE(), rd.department_id  = st.department_id , rd.department_name  = st.department_name , rd.allocated_budget = st.allocated_budget , 
                rd.department_head_id = st.department_head_id 
                 FROM raw.raw_temp_departments st 
                 WHERE rd.department_id  = st.department_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_departments rd SET rd._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_departments st 
                 where rd.department_id  = st.department_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_departments','raw_temp_departments')"
               ] 
            )}}
        
with st_departments as (
     select * from {{ref('raw_temp_departments')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_departments as (
    select department_id , department_name , allocated_budget , department_head_id ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    department_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(department_id as string), '!@|@!'), '~#~',
    ifnull(cast(department_name as string), '!@|@!'), '~#~',
    ifnull(cast(allocated_budget as string), '!@|@!'), '~#~',
    ifnull(cast(department_head_id as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_departments, elt_logs el
),  

final as (
    select * from raw_departments
)

select * from final  