with tfs_transaction_account as (
    select * from {{ref('tfs_transaction_accounts')}}
) ,

raw_accounts as (
select * from {{ref('raw_accounts')}}
),

tfs_transaction_accounts as (
    select tta.* , ra.account_open_date 
    from tfs_transaction_account tta 
    join raw_accounts ra
    on tta.account_id = ra.account_id
)
select * from tfs_transaction_accounts
