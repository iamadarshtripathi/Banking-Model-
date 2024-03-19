{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_departments')"]) }}
with st_departments as (
    select * from {{source('STAGING','ST_DEPARTMENTS')}}
),
final as (
    select * from st_departments
)
select * from final