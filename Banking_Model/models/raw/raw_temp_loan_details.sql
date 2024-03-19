{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_loan_details')"]) }}
with st_loan_details as (
    select * from {{source('STAGING','ST_LOAN_DETAILS')}}
),
final as (
    select * from st_loan_details
)
select * from final
