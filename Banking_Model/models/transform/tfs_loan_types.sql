-- config to make base table transaction_types not appendable.

{{config(materialized= "table")}}
-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(post_hook=[
                "UPDATE transform.tfs_loan_types r SET r._ELT_Updated = GETDATE() ,
                r.loan_type_id = st.loan_type_id,
                r.loan_type = st.loan_type,
                r.gender = st.gender,
                r.cibil_score = st.cibil_score,
                r.time_duration = st.time_duration,
                r.interest_rate = st.interest_rate
                    FROM RAW.raw_temp_loan_types st 
                    WHERE r.loan_type_id = st.loan_type_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE transform.tfs_loan_types r SET r._ELT_Is_Deleted = 1 
                    FROM RAW.raw_temp_loan_types st 
                    where r.loan_type_id = st.loan_type_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    "call raw.sp_Update_elt_logs('tfs_loan_types','raw_temp_loan_types')"
                    
               ] 
            )}}


-- source for accessing staging loan_types table data.
with st_loan_types as(
    select * from {{ref('raw_temp_loan_types')}} where metadata$action = 'INSERT' and metadata$isupdate = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

elt_columns as (
  select  loan_type_id,loan_type,gender,cibil_score,time_duration,interest_rate,
    GETDATE() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    false as _elt_is_deleted,
    loan_type_id as _elt_joinkey,
    'faker' as _elt_source_system,
     sha2(concat(ifnull(cast(loan_type_id as string), '!@|@!'), '~#~',
    ifnull(cast(loan_type as string), '!@|@!'), '~#~',
    ifnull(cast(gender as string), '!@|@!'), '~#~',
    ifnull(cast(cibil_score as string), '!@|@!'), '~#~',
    ifnull(cast(time_duration as string), '!@|@!'), '~#~',
    ifnull(cast(interest_rate as string), '!@|@!'), '~#~' ),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_loan_types, elt_logs el

),
final as (
    select * from elt_columns

)
select * from final

