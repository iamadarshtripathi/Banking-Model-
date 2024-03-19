-- This is view is created to get the details of Zelle performance


with zelle_transaction as (
   select TRANSACTION_ID , ACCOUNT_ID , FORMAT_ID , TRANSACTION_TYPE_ID , SENDER , RECEIVER ,
   TRANSACTION_DATE , PROCESS_DATE , PROCESS_STATUS , CURRENCY_CODE , TRANSACTION_AMOUNT
   from {{ref('raw_transactions')}}
   where transaction_type_id = 15 
),

raw_branches as (
      select rb.*, sa.region 
      from {{ref('raw_branches')}} as rb 
      left join {{source('REF','STATESABBREVATIONS')}} as sa
      on rb.branch_state = sa.state
),

account_branch as (

select ra.account_id , ra.branch_id , rb.branch_name , rb.branch_city , rb.branch_state , rb.region 
from  {{ref('raw_accounts')}} as ra join
raw_branches as rb on
ra.branch_id = rb.branch_id
),

success_failed as (
select * ,
case
   when PROCESS_STATUS = 'Successful' then 1
   else 0
   end as Success ,
case
   when PROCESS_STATUS = 'Failed' then 1
   else 0
   end as Failed ,
case 
    when FORMAT_ID = 1 then 0
    else 1
    end as outgoing_transaction

from zelle_transaction
),

transaction_branch as (
select sf.* , ab.branch_id ,ab.branch_name , ab.branch_city , ab.region , 
ab.branch_state
from  account_branch ab join
success_failed sf on 
sf.account_id = ab.account_id
)

select * from transaction_branch