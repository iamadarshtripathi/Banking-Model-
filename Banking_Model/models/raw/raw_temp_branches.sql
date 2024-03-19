{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_branches')"]) }}

with st_branches as(
    select * from {{source('STAGING','ST_BRANCHES')}}
),

final as (
    select * from st_branches
)

select * from final