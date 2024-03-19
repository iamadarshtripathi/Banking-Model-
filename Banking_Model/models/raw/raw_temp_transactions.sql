{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_transactions')"]) }}
with stage_loan_transactions as (
    select  * from {{source('STAGING','ST_TRANSACTIONS')}} where METADATA$ACTION='INSERT'
)

select * from stage_loan_transactions