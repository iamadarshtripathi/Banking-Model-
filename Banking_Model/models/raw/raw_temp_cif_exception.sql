{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_cif_exception')"]) }}

with st_cif_exception as(
    select * from {{source('STAGING','ST_CIF_EXCEPTION')}}
),

final as (
    select * from st_cif_exception
)

select * from final