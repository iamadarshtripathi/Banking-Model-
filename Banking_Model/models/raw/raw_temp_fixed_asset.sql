{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_fixed_asset')"]) }}
with st_fixed_asset as (
    select * from {{source('STAGING','ST_FIXED_ASSET')}}
),
final as (
    select * from st_fixed_asset
)
select * from final