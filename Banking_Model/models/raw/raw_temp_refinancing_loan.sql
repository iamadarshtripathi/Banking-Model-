{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_refinancing_loan')"]) }}

with st_refinancing_loan as (
    select * from {{source('STAGING','ST_REFINANCING_LOAN')}}
),
final as (
    select * from st_refinancing_loan
)
select * from final