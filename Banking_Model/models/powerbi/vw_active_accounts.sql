--This view will provide all the information regarding active accounts 

-- Select all the active accounts 
with active_accounts as (
select * from {{ref('raw_accounts')}} where account_status = 'Open' 
),

--adding description to the accounts

account_types as (
select aa.* , rat.account_type_description ,rat.account_type from active_accounts aa 
join {{ref('raw_account_types')}}  rat 
on rat.account_type_id = aa.account_type_id
),

--adding branch data from where account was opened

account_open_region as  (

  select at.* , rb.branch_state from account_types at 
  join {{ref('raw_branches')}}  rb 
  on at.branch_id = rb.branch_id 

),

--checking account balance

account_balance as (
select aor.* , rta.current_balance from account_open_region aor 
join {{ref('tfs_transaction_accounts')}}  rta 
on aor.account_id = rta.account_id
),

final as (
select * from account_balance
)

select * from final

