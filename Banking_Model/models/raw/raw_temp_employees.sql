{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_employees')"]) }}
with st_employees as (
    select * from {{source('STAGING','ST_EMPLOYEES')}}
),
final as (
    select * from st_employees
)
select * from final
