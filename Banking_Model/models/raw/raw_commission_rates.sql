{{config(tags=["p3"])}}

with stage_commission_rates as (
    select * from {{source('STAGING','COMMISSION_RATES')}}
),

final as (
    select * from stage_commission_rates
)

select * from final