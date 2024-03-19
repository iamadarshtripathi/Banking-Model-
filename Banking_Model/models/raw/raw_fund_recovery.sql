{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_fund_recovery rfr SET rfr._ELT_Updated = GETDATE(), rfr.collected = st.collected , rfr.brokerage = st.brokerage , rfr.assured = st.assured ,  rfr.target = st.target ,
                 rfr.calls_completed = st.calls_completed ,rfr.calls_remaining = st.calls_remaining , rfr.date = st.date ,
                 rfr.city = st.city
                 FROM raw.raw_temp_fund_recovery st 
                 WHERE rfr.collected = st.collected
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_fund_recovery rfr SET rfr._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_fund_recovery st 
                 where rfr.collected = st.collected
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_fund_recovery','raw_temp_fund_recovery')"
               ] 
            )}}

with st_fund_recovery as (
     select * from {{ref('raw_temp_fund_recovery')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),

raw_fund_recovery as  (
    select collected , brokerage , assured , target , calls_completed , calls_remaining , date ,city , 
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted,
    collected as _elt_joinkey,
     sha2(concat(ifnull(cast(collected as string),'!@|@!'),'~#~', 
     ifnull(cast(brokerage as string),'!@|@!'),'~#~', 
     ifnull(cast(assured as string),'!@|@!'),'~#~', 
     ifnull(cast(target as string),'!@|@!'),'~#~',
     ifnull(cast(calls_completed as string),'!@|@!'),'~#~',
     ifnull(cast(calls_remaining as string),'!@|@!'),'~#~',
     ifnull(cast(date as string),'!@|@!'),'~#~',
     ifnull(cast(city as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_fund_recovery, elt_logs el
),

final as (
    select * from raw_fund_recovery
)

select * from final 
