{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_fixed_deposit')"]) }}
with st_fixed_deposit as (
    select * from {{source('STAGING','ST_FIXED_DEPOSIT')}}
),
final as (
    select * from st_fixed_deposit
)
select * from final
