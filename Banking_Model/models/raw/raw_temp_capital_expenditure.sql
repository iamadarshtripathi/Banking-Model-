{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_capital_expenditure')"]) }}
with st_capital_expenditure as (
    select * from {{source('STAGING','ST_CAPITAL_EXPENDITURE')}}
),
final as (
    select * from st_capital_expenditure
)
select * from final