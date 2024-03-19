-- This view is generated to get all the retail performance of bank
-- accounts opened, loans distributed, credit card issued and many more 

with raw_account as(
select account_id,
       account_type_id,
       account_number,
       customer_id,
       branch_id,
       account_open_date
from {{ref('raw_accounts')}}
),

raw_account_type as(
select ra.account_id,
       ra.account_type_id,
       ra.account_number,
       ra.customer_id,
       ra.branch_id,
       ra.account_open_date,
       rat.account_type,
       rat.account_type_description
from raw_account ra
left join {{ref('raw_account_types')}} as rat
on ra.account_type_id = rat.account_type_id
),

account_type_loan_details as(
select raa.*,
       rld.loan_id,
       rld.sanction_date,
       rld.loan_amount,
       rld.loan_type_id,
       rld.tenure,
       rld.emi_amount
from raw_account_type raa
left join {{ref('raw_loan_details')}} as rld
on raa.account_id = rld.account_number
),

loan_details_card_details as(
select atld.*,
       cc.card_id,
       cc.card_type,
       cc.card_number,
       cc.card_limit,
       cc.issued_date,
       cc.valid_date,
       cc.card_sub_type
from account_type_loan_details atld
left join {{ref('raw_card_details')}} as cc
on atld.customer_id = cc.customer_id
),

credit_card_branches as(
select ldc.*,
       br.branch_city,
       br.branch_state
from loan_details_card_details ldc
left join {{ref('raw_branches')}} as br
on br.branch_id = ldc.branch_id
),

raw_transaction_account as(
select ccb.*,
       rta.current_balance,
       rta.available_balance
from credit_card_branches as ccb
left join {{ref('tfs_transaction_accounts')}} as rta
on ccb.account_id = rta.account_id
),

region as (
select rta.*,
       rsa.region
from raw_transaction_account as rta
left join {{source('REF','STATESABBREVATIONS')}} as rsa
on rta.branch_state = rsa.state
)

select * from region