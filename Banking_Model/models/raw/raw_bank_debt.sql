{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_bank_debt rbd SET rbd._ELT_Updated = GETDATE() ,rbd.debt_id = st.debt_id, rbd.debtor_name = st.debtor_name , rbd.debt_type = st.debt_type , rbd.principal_amount = st.principal_amount , 
                rbd.interest_rate = st.interest_rate , rbd.issue_date = st.issue_date , rbd.maturity_date = st.maturity_date , rbd.collateral = st.collateral , rbd.guarantee = st.guarantee ,rbd.debt_status = st.debt_status
                 FROM raw.raw_temp_bank_debt st 
                 WHERE rbd.debt_id = st.debt_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                  
                "UPDATE raw.raw_bank_debt rbd SET rbd._ELT_Is_Deleted = 1 
                FROM raw.raw_temp_bank_debt st 
                where rbd.debt_id = st.debt_id 
                and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                "call raw.sp_Update_elt_logs('raw_bank_debt','raw_temp_bank_debt')"
                ] 
            )}}

with st_bank_debt as(
    select * from {{ref('raw_temp_bank_debt')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
 

raw_bank_debt as (
    select debt_id , debtor_name , debt_type , principal_amount , 
    interest_rate , issue_date , maturity_date , collateral , 
    guarantee , debt_status,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted ,
    debt_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(debt_id as string),'!@|@!'),'~#~', 
     ifnull(cast(debtor_name as string),'!@|@!'),'~#~', 
     ifnull(cast(debt_type as string),'!@|@!'),'~#~',
     ifnull(cast(principal_amount as string),'!@|@!'),'~#~',
     ifnull(cast(interest_rate as string),'!@|@!'),'~#~',
     ifnull(cast(issue_date as string),'!@|@!'),'~#~',
     ifnull(cast(maturity_date as string),'!@|@!'),'~#~',
     ifnull(cast(collateral as string),'!@|@!'),'~#~',
     ifnull(cast(guarantee as string),'!@|@!'),'~#~',
     ifnull(cast(debt_status as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_bank_debt , elt_logs el
),

final as (
    select * from raw_bank_debt
)

select * from final
