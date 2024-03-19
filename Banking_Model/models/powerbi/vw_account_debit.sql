with raw_account as(
select ra.account_id,
       ra.account_type_id,
       ra.account_open_date,
       ra.account_close_date,
       ra.account_status,
       ra.branch_id
from {{ref('raw_accounts')}} as ra
),

raw_account_type as(
select raw_account.*,
       at.account_type, 
       at.account_type_description
from raw_account
join {{ref('raw_account_types')}} as at
on raw_account.account_type_id = at.account_type_id
),

raw_card_detail as(
select raw_account_type.*,
       rcd.card_id,
       rcd.card_type,
       rcd.card_sub_type,
       rcd.issued_date,
       rcd.valid_date
from raw_account_type
join {{ref('raw_card_details')}} as rcd
on raw_account_type.account_id = rcd.account_id
)
select * from raw_card_detail