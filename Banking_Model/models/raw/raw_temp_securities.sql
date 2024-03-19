{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_securities')"]) }}
with st_securities as (
    select * from {{source('STAGING','ST_SECURITIES')}}
),
final as (
    select * from st_securities
)
select * from final