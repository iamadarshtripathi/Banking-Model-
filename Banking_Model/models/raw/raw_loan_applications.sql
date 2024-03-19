-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_loan_applications  rc SET rc._elt_updated = GETDATE() ,rc.application_id = st.application_id , rc.property_id = st.property_id ,
                 rc.loan_type_id = st.loan_type_id , rc.loan_amount = st.loan_amount ,
                 rc.customer_id = st.customer_id , rc.account_id = st.account_id , rc.applied_date = st.applied_date, rc.employee_id = st.employee_id 
                    FROM raw.raw_temp_loan_applications st 
                    WHERE rc.application_id = st.application_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                    
                "UPDATE raw.raw_loan_applications rc SET rc._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_loan_applications st 
                    where rc.application_id = st.application_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                    "call raw.sp_Update_elt_logs('raw_loan_applications','raw_temp_loan_applications')"
               ] 
            )}}

with st_loan_applications as(
    select * from {{ref('raw_temp_loan_applications')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
raw_loan_applications as (
     select application_id , property_id , loan_type_id , loan_amount , customer_id , account_id ,
    applied_date, employee_id , 
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated,
    false as _elt_is_deleted,
    application_id as _elt_joinkey,
    SHA2(CONCAT(IFNULL(CAST(APPLICATION_ID AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(PROPERTY_ID AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(LOAN_TYPE_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(LOAN_AMOUNT AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(ACCOUNT_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(APPLIED_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(EMPLOYEE_ID AS STRING),'!@|@!'),'~#~'),256)  AS _ELT_HASHKEY,  
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_loan_applications, elt_logs el    
),

final as (
    select * from raw_loan_applications
)

select * from final