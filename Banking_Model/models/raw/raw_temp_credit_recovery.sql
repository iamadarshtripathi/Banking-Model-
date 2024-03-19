{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_credit_recovery')"]) }}

with st_credit_recovery as(
    select * from {{source('STAGING','ST_CREDIT_RECOVERY')}}
),

final as (
    select * from st_credit_recovery
)

select * from final