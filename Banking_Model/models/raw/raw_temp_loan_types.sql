{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_loan_types')"]) }}

with stage_loan_types as(
    select * from {{source('STAGING','ST_LOAN_TYPES')}}
)

select * from stage_loan_types