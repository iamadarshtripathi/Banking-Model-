{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_transaction_formats')"]) }}

with stage_transaction_formats as(
    select * from {{source('STAGING','ST_TRANSACTION_FORMATS')}}
)

select * from stage_transaction_formats