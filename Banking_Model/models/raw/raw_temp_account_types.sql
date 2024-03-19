{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_account_types')" ,]
 )}}

with st_account_types as(
    select * from {{source('STAGING','ST_ACCOUNT_TYPES')}}
),

final as (
    select * from st_account_types
)

select * from final