with raw_accounts_table as (
select account_id,account_status,account_type_id,customer_id
from {{ref("raw_accounts")}}
),
raw_customers_table as (
select customer_id,customer_city,customer_zip
from {{ref('raw_customers')}}
),
tfs_transaction_accounts_table as (
select account_id,current_balance
from {{ref('tfs_transaction_accounts')}}
),
raw_account_types_table as (
select account_type_id,account_type_description
from {{ref('raw_account_types')}}
),
accounts_detail as (
select account_id,account_status,customer_id,account_type_description
from raw_accounts_table rat join 
raw_account_types_table ratt on rat.account_type_id=ratt.account_type_id
),
transactions_accounts as (
select ad.account_id as account_id,account_status,customer_id,account_type_description,current_balance
from accounts_detail ad join
tfs_transaction_accounts_table rtat on ad.account_id=rtat.account_id
),
final as (
select distinct account_id,account_status,ta.customer_id as customer_id,account_type_description,current_balance,customer_city,customer_zip
from transactions_accounts ta join
raw_customers_table rct on ta.customer_id=rct.customer_id
)
select * from final

