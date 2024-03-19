{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_customer_satisfaction')"]) }}

with st_customer_satisfaction as (
    select * from {{source('STAGING','ST_CUSTOMER_SATISFACTION')}}
),
final as (
    select * from st_customer_satisfaction
)
select * from final