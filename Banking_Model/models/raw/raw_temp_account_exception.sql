{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_account_exception')"]) }}

with st_account_exception as(
    select * from {{source('STAGING','ST_ACCOUNT_EXCEPTION')}}
),

final as (
    select * from st_account_exception
)

select * from final