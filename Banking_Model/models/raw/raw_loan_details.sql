{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_loan_details r SET r._ELT_Updated = CURRENT_TIMESTAMP() ,
                    r.LOAN_ID=st.LOAN_ID,
                    r.EMPLOYEE_ID=st.EMPLOYEE_ID,
                    r.ACCOUNT_NUMBER=st.ACCOUNT_NUMBER,
                    r.SANCTION_DATE=st.SANCTION_DATE,
                    r.LOAN_AMOUNT=st.LOAN_AMOUNT,
                    r.LOAN_TYPE_ID= st.LOAN_TYPE_ID,
                    r.TENURE=st.TENURE,
                    r.EMI_AMOUNT = st.EMI_AMOUNT,
                    r.APPLICATION_ID = st.APPLICATION_ID,
                    r.TRANSFER_ID = st.TRANSFER_ID
                    FROM  raw_temp_loan_details st 
                    WHERE r.LOAN_ID = st.LOAN_ID 
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,

                "UPDATE raw.raw_loan_details r SET r._ELT_Is_Deleted = 1 
                    FROM raw_temp_loan_details st 
                    where r.LOAN_ID = st.LOAN_ID 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                
                "call raw.sp_Update_elt_logs('raw_loan_details','raw_temp_loan_details')"
               ] 
            )}}

--getting data from loan temp details 
with loan_details as (
    select  * from {{ref('raw_temp_loan_details')}}
),

--getting current logs id
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

--setting log id with elt elements in loan details
loan_details_with_elt_columns as (
 select  LOAN_ID, EMPLOYEE_ID, ACCOUNT_NUMBER, SANCTION_DATE, LOAN_AMOUNT, LOAN_TYPE_ID, TENURE, EMI_AMOUNT
        , APPLICATION_ID, TRANSFER_ID, 
    case 
       when (METADATA$ACTION = 'INSERT' and metadata$isupdate = false) then GETDATE() 
    end as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    false as _elt_is_deleted,
    LOAN_ID as _elt_joinkey,
    sha2(concat(ifnull(cast(LOAN_ID as string), '!@|@!'), '~#~',
    ifnull(cast(EMPLOYEE_ID as string), '!@|@!'), '~#~',
    ifnull(cast(ACCOUNT_NUMBER as string), '!@|@!'), '~#~',
    ifnull(cast(SANCTION_DATE as string), '!@|@!'), '~#~',
    ifnull(cast(LOAN_AMOUNT as string), '!@|@!'), '~#~',
    ifnull(cast(LOAN_TYPE_ID as string), '!@|@!'), '~#~',
    ifnull(cast(EMI_AMOUNT as string), '!@|@!'), '~#~',
    ifnull(cast(APPLICATION_ID as string), '!@|@!'), '~#~',
    ifnull(cast(TRANSFER_ID as string), '!@|@!'), '~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from loan_details, elt_logs el
), 

--setting final cte
final as ( 
    select * from loan_details_with_elt_columns
)

--loading data in table
select * from final