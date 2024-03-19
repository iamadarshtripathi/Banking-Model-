{ { config(
    tags = ["p4"]
) } }


with stage_transaction as(
  select  transaction_id , format_id , check_number , transaction_amount , _elt_is_deleted , _elt_run_id , _elt_updated , _elt_source_system from {{ref('raw_transactions')}}
  where
    transaction_type_id = 12
),


stage_check_transaction as (
  select
    transaction_id as check_transaction_id,
    transaction_id,
    check_number,
    case
      when stage_transaction.format_id = 1 then stage_transaction.transaction_amount
      else null
    end as credit,
    case
      when stage_transaction.format_id <> 1 then stage_transaction.transaction_amount
      else null
    end as debit ,
    _elt_is_deleted,
    _elt_run_id,
    _elt_updated,
    _elt_source_system
  from
    stage_transaction
),




final as (
  select *,
  GETDATE() as _elt_inserted ,
  check_transaction_id as _elt_joinkey,
  sha2(concat(ifnull(cast(check_transaction_id as string), '!@|@!'),'~#~', 
  ifnull(cast(transaction_id as string), '!@|@!'),'~#~', 
  ifnull(cast(check_number as string), '!@|@!'),'~#~',
  ifnull(cast(credit as string), '!@|@!'),'~#~',
  ifnull(cast(debit as string), '!@|@!'),'~#~'), 256) _elt_hashkey
  from stage_check_transaction 


)

select * from final