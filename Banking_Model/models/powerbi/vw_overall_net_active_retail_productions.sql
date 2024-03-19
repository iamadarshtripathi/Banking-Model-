with active_accounts as (
select * from {{ref('raw_accounts')}}  where account_status = 'Open' 
),

account_types as (
select aa.* ,rat.account_type , rat.account_type_description  from active_accounts aa 
join {{ref('raw_account_types')}} rat 
on rat.account_type_id = aa.account_type_id
),

balance as (
select at.* , rta.current_balance as current_balance from account_types at
join {{ref('tfs_transaction_accounts')}}  rta 
on at.account_id = rta.account_id
),

branches as (
select b.account_id , b.account_type_id , b.account_open_date , b.branch_id , b.account_type , b.account_type_description ,b.current_balance,  br.branch_state
from balance b join {{ref('raw_branches')}}  br 
on b.branch_id = br.branch_id

),

final as (
select * from branches
)
select * from final
