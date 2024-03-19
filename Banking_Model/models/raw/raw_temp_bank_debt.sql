{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_bank_debt')"]) }}
with st_bank_debt as (
    select * from {{source('STAGING','ST_BANK_DEBT')}}
),
final as (
    select * from st_bank_debt
)
select * from final
