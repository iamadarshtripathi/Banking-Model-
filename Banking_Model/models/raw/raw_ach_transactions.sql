-- filtered out ach_transactions from transactions table
{{ config(tags = ["p4"]) }} 
with stage_transaction as(
  select
    ach_addenda,
    transaction_id,
    transaction_amount,
    currency_code,
    format_id,
    _elt_is_deleted,
    _elt_run_id,
    _elt_updated,
    _elt_source_system
  from
    {{ ref('raw_transactions') }}
  where
    transaction_type_id = 11
),
-- generated ach_transaction table from filtered data 
-- case statement: adding transaction amount to either debit or credit column depeding upon format_type
raw_ach_transactions as (
  select
    stage_transaction.transaction_id as ach_transaction_id,
    transaction_id,
    currency_code,
    ach_addenda,
    case
      when stage_transaction.format_id = 1 then stage_transaction.transaction_amount
      else null
    end as credit,
    case
      when stage_transaction.format_id <> 1 then stage_transaction.transaction_amount
      else null
    end as debit,
    _elt_is_deleted,
    _elt_run_id,
    _elt_updated,
    _elt_source_system
  from
    stage_transaction
),
final as (
  select
    *,
    GETDATE() as _elt_inserted,
    ach_transaction_id as _elt_joinkey,
    SHA2(
      CONCAT(
        IFNULL(CAST(ACH_TRANSACTION_ID AS STRING), '!@|@!'),
        '~#~',
        IFNULL(CAST(TRANSACTION_ID AS STRING), '!@|@!'),
        '~#~',
        IFNULL(CAST(CURRENCY_CODE AS STRING), '!@|@!'),
        '~#~',
        IFNULL(CAST(ACH_ADDENDA AS STRING), '!@|@!'),
        '~#~',
        IFNULL(CAST(CREDIT AS STRING), '!@|@!'),
        '~#~',
        IFNULL(CAST(DEBIT AS STRING), '!@|@!'),
        '~#~'
      ),
      256
    ) as _ELT_HashKey
  from
    raw_ach_transactions
)
select * from final