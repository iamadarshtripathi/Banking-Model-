{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_map_auto_loan')"]) }}
with st_auto_info as (
    select * from {{source('STAGING','ST_MAP_AUTO_LOAN')}}
),
final as (
    select * from st_auto_info
)
select * from final