{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_mortgages_info')"]) }}
with st_mortgages_info as (
    select * from {{source('STAGING','ST_MORTGAGES_INFO')}}
),
final as (
    select * from st_mortgages_info
)
select * from final
