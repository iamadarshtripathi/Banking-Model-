{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_uninsured_deposits')"]) }}
with stage_uninsured_deposits as (
    select  * from {{source('STAGING','ST_UNINSURED_DEPOSITS')}} where METADATA$ACTION='INSERT'
)

select * from stage_uninsured_deposits