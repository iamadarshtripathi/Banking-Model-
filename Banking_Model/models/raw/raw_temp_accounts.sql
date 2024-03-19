{{ config(materialized = "table",tags=["p2"],
pre_hook = [ "call raw.sp_check_and_drop_table('raw_temp_accounts')"] ) 
}}

with st_account as(
    select * from {{source('STAGING','ST_ACCOUNTS')}}
),

final as (
    select * from st_account
)

select * from final