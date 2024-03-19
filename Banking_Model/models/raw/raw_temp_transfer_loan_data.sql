{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_transfer_loan_data')"]) }}
with stage_transfer_loan_data as (
    select  * from {{source('STAGING','ST_TRANSFER_LOAN_DATA')}} where METADATA$ACTION='INSERT'
)

select * from stage_transfer_loan_data