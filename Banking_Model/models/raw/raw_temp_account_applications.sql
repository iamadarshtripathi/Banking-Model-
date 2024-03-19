{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_account_applications')"]) }}

with st_account_applications as(
    select * from {{source('STAGING','ST_ACCOUNT_APPLICATIONS')}}
),

final as (
    select * from st_account_applications
)

select * from final