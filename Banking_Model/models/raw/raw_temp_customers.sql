{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_customers')"]) }}

with st_customers as(
    select * from {{source('STAGING','ST_CUSTOMERS')}}
),

final as (
    select * from st_customers
)

select * from final