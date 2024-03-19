{{config(tags=["p4"],post_hook=[
             "UPDATE raw.raw_uninsured_deposits rud SET rud._ELT_Updated = GETDATE() ,
             rud.non_performing_account_id = st.non_performing_account_id,
             rud.non_performing_amount = st.non_performing_amount,
             rud.tier1 = st.tier1,
             rud.risk_weighted_assets = st.risk_weighted_assets,
             rud.created_date = st.created_date,
             rud.account_id = st.account_id,    
             rud.description = st.description
             FROM raw.raw_temp_uninsured_deposits st
             WHERE rud.account_id = st.account_id
             AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
             "UPDATE raw.raw_uninsured_deposits rud SET rud._ELT_Is_Deleted = 1
             FROM raw.raw_temp_uninsured_deposits st
             where rud.account_id = st.account_id
             and st.METADATA$ACTION = 'DELETE' and st.METADATA$ISUPDATE = False"
            ]

)}}

with st_uninsured_deposits as (
    select * from {{ ref('raw_temp_uninsured_deposits') }} where metadata$action = 'INSERT' and metadata$isupdate = false
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),

raw_uninsured_deposits as(
    select non_performing_account_id,non_performing_amount,tier1,risk_weighted_assets,
    created_date,account_id,description,
    current_timestamp() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    False as _elt_is_deleted,
    account_id as _elt_joinkey,
    sha2(concat(ifnull(cast(non_performing_account_id as string),'!@|@!'),'~#~', 
                ifnull(cast(non_performing_amount as string),'!@|@!'),'~#~',
                ifnull(cast(tier1 as string),'!@|@!'),'~#~',
                ifnull(cast(risk_weighted_assets as string),'!@|@!'),'~#~',
                ifnull(cast(created_date as string),'!@|@!'),'~#~',
                ifnull(cast(account_id as string),'!@|@!'),'~#~',
                ifnull(cast(description as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_uninsured_deposits, elt_logs el
),

final as (
    select * from raw_uninsured_deposits
)

select * from final