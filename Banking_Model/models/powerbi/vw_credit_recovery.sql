with raw_credit_recovery as(
select rcr.credit_index,
       rcr.scheduled_calls,
       rcr.scheduled_emails,
       rcr.interest,
       rcr.paid,
       rcr.due,
       rcr.balance,
       rcr.date,
       rcr.city
from {{ref('raw_credit_recovery')}} as rcr
)

select * from raw_credit_recovery