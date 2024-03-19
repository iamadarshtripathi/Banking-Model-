{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_expense')"]) }}
with st_expense as (
    select * from {{source('STAGING','ST_EXPENSE')}}
),
final as (
    select * from st_expense
)
select * from final