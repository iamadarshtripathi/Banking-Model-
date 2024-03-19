{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_non_performing_loan')"]) }}

with st_non_performing_loan as (
    select * from {{source('STAGING','ST_NON_PERFORMING_LOAN')}}
),
final as (
    select * from st_non_performing_loan
)
select * from final