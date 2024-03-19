{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_loan_applications')"]) }}

with st_loan_applications as(
    select * from {{source('STAGING','ST_LOAN_APPLICATIONS')}}
),

final as (
    select * from st_loan_applications
)

select * from final