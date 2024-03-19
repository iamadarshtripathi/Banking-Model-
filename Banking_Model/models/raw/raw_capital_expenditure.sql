{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_capital_expenditure rce SET rce._ELT_Updated = GETDATE(), rce.expenditure_id  = st.expenditure_id , rce.asset_id  = st.asset_id , rce.expenditure_date = st.expenditure_date , 
                rce.expenditure_amount = st.expenditure_amount 
                 FROM raw.raw_temp_capital_expenditure st 
                 WHERE rce.expenditure_id  = st.expenditure_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_capital_expenditure rce SET rce._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_capital_expenditure st 
                 where rce.expenditure_id  = st.expenditure_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_capital_expenditure','raw_temp_capital_expenditure')"
               ] 
            )}}
        
with st_capital_expenditure as (
     select * from {{ref('raw_temp_capital_expenditure')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_capital_expenditure as (
    select expenditure_id , asset_id , expenditure_date , expenditure_amount ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    expenditure_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(expenditure_id as string), '!@|@!'), '~#~',
    ifnull(cast(asset_id as string), '!@|@!'), '~#~',
    ifnull(cast(expenditure_date as string), '!@|@!'), '~#~',
    ifnull(cast(expenditure_amount as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_capital_expenditure, elt_logs el
),  

final as (
    select * from raw_capital_expenditure
)

select * from final  