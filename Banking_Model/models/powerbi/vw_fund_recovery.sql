with raw_fund_recovery as(
select rfr.collected,
       rfr.brokerage,
       rfr.assured,
       rfr.target,
       rfr.calls_completed,
       rfr.calls_remaining,
       rfr.date,
       rfr.city
from {{ref('raw_fund_recovery')}} as rfr
)

select * from raw_fund_recovery