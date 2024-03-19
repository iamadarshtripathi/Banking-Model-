{{ config(materialized = "table",tags=["p2"],
pre_hook=["call raw.sp_check_and_drop_table('raw_temp_card_details')"]) }}
with st_card_details as(
    select * from {{source('STAGING','ST_CARD_DETAILS')}}
),

final as (
    select * from st_card_details
)

select * from final