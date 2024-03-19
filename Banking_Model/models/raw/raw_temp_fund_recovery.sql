{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_fund_recovery')"]) }}

with st_fund_recovery as(
    select * from {{source('STAGING','ST_FUND_RECOVERY')}}
),

final as (
    select * from st_fund_recovery
)

select * from final