


with security as (
  select security_id , security_name , security_type , quantity as security_quantity, 
  market_value , purchase_date , purchase_price , purchase_value
  from {{ref('raw_securities')}}
)

select * from security
