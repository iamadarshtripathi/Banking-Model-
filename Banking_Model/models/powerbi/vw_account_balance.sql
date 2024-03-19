-- This is view is created to get the details of balances of accounts 


with raw_accounts as (
   select * from {{ref('raw_accounts')}}
),

raw_customers as (
    select * from {{ref('raw_customers')}}
),

raw_branches as (
   select * from {{ref('raw_branches')}}
),

raw_transaction_accounts as (
    select * from {{ref('tfs_transaction_accounts')}}
),

join_table as (
  select ra.account_id , ra.customer_id , ra.branch_id , ra.account_open_date , rta.current_balance ,
  case 
    when ra.account_status = 'Open' and rta.current_balance > 0 then 1
    else 0
    end as active_open_account
  from raw_accounts ra
  join raw_transaction_accounts rta
  on ra.account_id = rta.account_id 
  where ra.account_status = 'Open' and rta.current_balance > 0 or ra.account_status = 'Close' and rta.current_balance > 0
),

account_branches as (
    select jt.* ,rb.branch_name , rb.branch_city , rb.branch_state
     from join_table jt
     join raw_branches rb
     on jt.branch_id = rb.branch_id
) ,

customers_account as (
   select ab.* , rc.customer_name
   from account_branches ab
   join raw_customers rc
   on ab.customer_id = rc.customer_id
)

select * from customers_account