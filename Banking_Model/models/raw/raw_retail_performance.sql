{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_retail_performance rrp SET rrp._ELT_Updated = GETDATE(), rrp.customer_id  = st.customer_id, rrp.date_col = st.date_col , rrp.location = st.location ,
                 rrp.customers = st.customers ,rrp.units = st.units ,rrp.balance = st.balance ,
                 rrp.loan_applications = st.loan_applications ,rrp.overtime_hours = st.overtime_hours ,rrp.overtime_dollars = st.overtime_dollars,
                 rrp.turnover_rate = st.turnover_rate,
                 rrp.employee_name = st.employee_name,
                 rrp.product_type = st.product_type, 
                 rrp.revenue = st.revenue,
                 rrp.credit_card_decision_status = st.credit_card_decision_status,
                 rrp.credit_card_type = st.credit_card_type,
                 rrp.deposit_account_status = st.deposit_account_status,
                 rrp.account_number = st.account_number
                 FROM raw.raw_temp_retail_performance st 
                 WHERE rrp.customer_id = st.customer_id 
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_retail_performance rrp SET rrp._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_retail_performance st 
                 where rrp.customer_id = st.customer_id 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_retail_performance','raw_temp_retail_performance')"
               ] 
            )}}
        
with st_retail_performance as (
     select * from {{ref('raw_temp_retail_performance')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),    

raw_retail_performance as (
    select customer_id , date_col , location , customers, units, balance, loan_applications, overtime_hours, overtime_dollars, 
    turnover_rate, employee_name, product_type, revenue, credit_card_decision_status, credit_card_type, deposit_account_status, account_number,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted ,
    customer_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(customer_id as string), '!@|@!'), '~#~',
    ifnull(cast(date_col as string), '!@|@!'), '~#~',
    ifnull(cast(location as string), '!@|@!'), '~#~',
    ifnull(cast(customers as string), '!@|@!'), '~#~',
    ifnull(cast(units as string), '!@|@!'), '~#~',
    ifnull(cast(balance as string), '!@|@!'), '~#~',
    ifnull(cast(loan_applications as string), '!@|@!'), '~#~',
    ifnull(cast(overtime_hours as string), '!@|@!'), '~#~',
    ifnull(cast(overtime_dollars as string), '!@|@!'), '~#~',
    ifnull(cast(turnover_rate as string), '!@|@!'), '~#~',
    ifnull(cast(employee_name as string), '!@|@!'), '~#~',
    ifnull(cast(product_type as string), '!@|@!'), '~#~',
    ifnull(cast(revenue as string), '!@|@!'), '~#~',
    ifnull(cast(credit_card_decision_status as string), '!@|@!'), '~#~',
    ifnull(cast(credit_card_type as string), '!@|@!'), '~#~',
    ifnull(cast(deposit_account_status as string), '!@|@!'), '~#~',
    ifnull(cast(account_number as string), '!@|@!'), '~#~'),256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_retail_performance, elt_logs el
),  

final as (
    select * from raw_retail_performance
)

select * from final       

