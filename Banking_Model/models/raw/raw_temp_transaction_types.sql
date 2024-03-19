{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_transaction_types')"]) }}

with stage_transaction_types as(
    select * from {{source('STAGING','ST_TRANSACTION_TYPES')}}
)

select * from stage_transaction_types