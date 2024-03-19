with raw_account as(
select account_id,
       account_type_id,
       account_open_date,
       account_close_date,
       account_status,
       customer_id,
       branch_id
from {{ref('raw_accounts')}} 
),

raw_account_type as(
select raw_account.*,
       at.account_type,
       at.account_type_description
from raw_account
join {{ref('raw_account_types')}} as at
on raw_account.account_type_id = at.account_type_id
),

raw_transaction_account as(
select raw_account_type.*,
       rta.transaction_account_id,
       rta.current_balance,
       rta.available_balance,
       rta.average_balance_mtd
from raw_account_type
join {{ref('tfs_transaction_accounts')}} as rta
on raw_account_type.account_id = rta.account_id
),

raw_branch as(
select raw_transaction_account.*,
       rb.branch_city,
       rb.branch_state
from raw_transaction_account
join {{ref('raw_branches')}} as rb
on raw_transaction_account.branch_id = rb.branch_id
),

region as(
select raw_branch.* ,
       st.region
from raw_branch
join {{source('REF','STATESABBREVATIONS')}} as st
on raw_branch.branch_state = st.state
)
select * from region