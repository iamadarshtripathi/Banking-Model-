-- This is view is created to get the details of online fees transaction



with raw_transactions as (
      select * from {{ref('raw_transactions')}}
),

transaction_id as (
    select transaction_id from raw_transactions
),

fees_amount as (
   select TRANSACTION_ID as fees_id , TRANSACTION_DATE as fees_transaction_date , TRANSACTION_AMOUNT as fees_amount
   from raw_transactions
   where format_id = 7 
),

online_payment as (
   select TRANSACTION_ID , TRANSACTION_DATE as online_transaction_date , TRANSACTION_AMOUNT as online_transaction_amount 
   from raw_transactions
   where transaction_type_id = 15
),

transaction_fees as (
   select case when ti.transaction_id = fa.fees_id then 1 else 0 end as fees_id , fa.fees_transaction_date , fa.fees_amount , ti.transaction_id
   from transaction_id ti
   left join fees_amount fa
   on ti.transaction_id = fa.fees_id
),

online_transaction as (
    select tf.* , case when tf.transaction_id = op.TRANSACTION_ID then 1 else 0 end as online_id ,  op.online_transaction_date , op.online_transaction_amount 
    from transaction_fees tf
    left join  online_payment op
    on tf.transaction_id = op.TRANSACTION_ID
)

select * from online_transaction