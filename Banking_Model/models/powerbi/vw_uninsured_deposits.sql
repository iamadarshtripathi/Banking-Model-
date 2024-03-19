with raw_uninsured_deposits as (
    select * from {{ref('raw_uninsured_deposits')}} 
)

select * from raw_uninsured_deposits