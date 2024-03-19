
-- This is view is created to get the details of account analysis fees


with fees_transaction as (
   select TRANSACTION_ID , ACCOUNT_ID , FORMAT_ID , TRANSACTION_TYPE_ID , SENDER , RECEIVER ,
   TRANSACTION_DATE , PROCESS_DATE , PROCESS_STATUS , CURRENCY_CODE , TRANSACTION_AMOUNT as FEES_AMOUNT
   from {{ref('raw_transactions')}}
   where format_id = 7 or  format_id = 1 and transaction_type_id = 20
),

accounts as (
  select account_id, branch_id, customer_id from {{ref('raw_accounts')}}
),

branches as (
  select branch_id, branch_state from {{ref('raw_branches')}}
),

customers as (
  select customer_id, customer_name from {{ref('raw_customers')}}
),

employees as (
  select employee_id ,first_name ,last_name, branch_id from {{ref('raw_employees')}} 
),

account_branch as (
  select ra.account_id , ra.branch_id , rb.branch_state
  from accounts as ra join
  branches as rb on
  ra.branch_id = rb.branch_id
),

transaction_types as (
  select transaction_type_id, transaction_type_description
   from {{ref('raw_transaction_types')}} 
),

rta as (
  select * from {{ref('tfs_transaction_accounts')}}
),

customer_account as (
    select ra.account_id , rc.customer_id , rc.customer_name
    from  accounts as ra join
    customers as rc on
    ra.customer_id = rc.customer_id
),

waived_hard_dollar as (
  select * , 
        case
          when format_id = 1 and transaction_type_id = 20 then 1
          else 0
          end as waived_fees_status,
          case
          when format_id = 1 and transaction_type_id = 20 then FEES_AMOUNT
          else 0
          end as waived_fees_amount,
        case 
            when format_id = 7 and transaction_type_id = 19 then 1
            else 0
            end as hd_fees_status,
          case 
            when format_id = 7 and transaction_type_id = 19 then FEES_AMOUNT
            else 0
            end as hd_fees_amount,
        case
            when format_id = 7 and transaction_type_id != 19 then 1
            else 0
            end as tm_fees_status,
        case
            when format_id = 7 and transaction_type_id != 19 then FEES_AMOUNT
            else 0
            end as tm_fees_amount    
    from fees_transaction 
),

fees_account_branch as (
  select whd.* , ab.branch_id ,ab.branch_state as sub_region
  from  waived_hard_dollar whd join
  account_branch ab on 
  whd.account_id = ab.account_id
),

account_region as (
  select fab.* , sa.region
  from fees_account_branch fab join 
  {{source('REF','STATESABBREVATIONS')}} as sa
  on fab.sub_region = sa.state
),

fees_customer_account as (
  select ar.* , ca.customer_id ,ca.customer_name
  from  account_region ar join
  customer_account ca on 
  ar.account_id = ca.account_id
),

tresury_services_officer as (
  select fca.* , re.employee_id , re.first_name , re.last_name 
  from  fees_customer_account fca join
  employees as re 
  on fca.branch_id = re.branch_id
),

services_type as (
  select tso.* , rtt.transaction_type_description
  from tresury_services_officer tso join
  transaction_types as rtt 
  on tso.transaction_type_id = rtt.transaction_type_id
),

account_balance as (
  select st.* , rta.current_balance as account_balance
  from services_type st join 
  rta
  on st.account_id = rta.account_id
)

select * from account_balance


