{{ config(materialized = "table", tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_check_book')"]) }}

with st_check_book as(
    select * from {{source('STAGING','ST_CHECK_BOOK')}}
)

select * from st_check_book