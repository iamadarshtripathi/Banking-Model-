{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_auto_info')"]) }}
with st_auto_info as (
    select * from {{source('STAGING','ST_AUTO_INFO')}}
),
final as (
    select * from st_auto_info
)
select * from final
