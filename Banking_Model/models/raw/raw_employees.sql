{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_employees re SET re._ELT_Updated = GETDATE(), re.employee_id = st.employee_id , re.first_name = st.first_name , re.last_name = st.last_name ,  re.email_address = st.email_address ,
                 re.employee_address_line1 = st.employee_address_line1 , re.employee_address_line2 = st.employee_address_line2 ,
                 re.residence_number = st.residence_number , re.employee_city = st.employee_city , re.employee_state = st.employee_state, re.employee_zip = st.employee_zip , 
                 re.phone_number = st.phone_number , re.grade = st.grade , re.tiered_level = st.tiered_level , re.branch_id = st.branch_id 
                 FROM raw.raw_temp_employees st 
                 WHERE re.employee_id = st.employee_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_employees re SET re._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_employees st 
                 where re.employee_id = st.employee_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_employees','raw_temp_employees')"
               ] 
            )}}

with st_employees as (
     select * from {{ref('raw_temp_employees')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),

raw_employees as  (
    select employee_id , first_name , last_name , email_address , employee_address_line1 , employee_address_line2 , residence_number , employee_city ,
     employee_state , employee_zip , phone_number , grade , tiered_level , branch_id , 
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted,
    employee_id as _elt_joinkey,
     sha2(concat(ifnull(cast(employee_id as string),'!@|@!'),'~#~', 
     ifnull(cast(first_name as string),'!@|@!'),'~#~', 
     ifnull(cast(last_name as string),'!@|@!'),'~#~', 
     ifnull(cast(email_address as string),'!@|@!'),'~#~',
     ifnull(cast(employee_address_line1 as string),'!@|@!'),'~#~',
     ifnull(cast(employee_address_line2 as string),'!@|@!'),'~#~',
     ifnull(cast(residence_number as string),'!@|@!'),'~#~',
     ifnull(cast(employee_city as string),'!@|@!'),'~#~',
     ifnull(cast(employee_state as string),'!@|@!'),'~#~',
     ifnull(cast(employee_zip as string),'!@|@!'),'~#~',
     ifnull(cast(phone_number as string),'!@|@!'),'~#~',
     ifnull(cast(grade as string),'!@|@!'),'~#~',
     ifnull(cast(tiered_level as string),'!@|@!'),'~#~',
     ifnull(cast(branch_id as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_employees, elt_logs el
),

final as (
    select * from raw_employees
)

select * from final 
