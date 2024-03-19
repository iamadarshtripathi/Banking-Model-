{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_fixed_asset rfa SET rfa._ELT_Updated = GETDATE() ,rfa.asset_id = st.asset_id , rfa.asset_type = st.asset_type ,
                 rfa.purchase_date = st.purchase_date , rfa.purchase_cost = st.purchase_cost, rfa.current_cost = st.current_cost
                 FROM raw.raw_temp_fixed_asset st 
                 WHERE rfa.asset_id = st.asset_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                  
                "UPDATE raw.raw_fixed_asset rfa SET rfa._ELT_Is_Deleted = 1 
                FROM raw.raw_temp_fixed_asset st 
                where rfa.asset_id = st.asset_id 
                and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                "call raw.sp_Update_elt_logs('raw_fixed_asset','raw_temp_fixed_asset')"
                ] 
            )}}


with st_fixed_asset as(
    select * from {{ref('raw_temp_fixed_asset')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
 

raw_fixed_asset as (
    select asset_id , asset_type , purchase_date , purchase_cost , current_cost,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted ,
    asset_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(asset_id as string),'!@|@!'),'~#~', 
     ifnull(cast(asset_type as string),'!@|@!'),'~#~', 
     ifnull(cast(purchase_date as string),'!@|@!'),'~#~',
     ifnull(cast(purchase_cost as string),'!@|@!'),'~#~',
     ifnull(cast(current_cost as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_fixed_asset , elt_logs el
),

final as (
    select * from raw_fixed_asset
)

select * from final

