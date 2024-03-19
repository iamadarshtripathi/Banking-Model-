{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_retail_performance')"]) }}

with st_retail_performance as (
    select * from {{source('STAGING','ST_RETAIL_PERFORMANCE')}}
),
final as (
    select * from st_retail_performance
)
select * from final