{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_securities rs SET rs._ELT_Updated = GETDATE() ,rs.security_id = st.security_id, rs.security_name = st.security_name , rs.security_type = st.security_type , 
                rs.quantity = st.quantity , rs.market_value = st.market_value , rs.purchase_date = st.purchase_date , rs.purchase_price = st.purchase_price , rs.purchase_value = st.purchase_value
                 FROM raw.raw_temp_securities st 
                 WHERE rs.security_id = st.security_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                  
                "UPDATE raw.raw_securities rs SET rs._ELT_Is_Deleted = 1 
                FROM raw.raw_temp_securities st 
                where rs.security_id = st.security_id 
                and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                "call raw.sp_Update_elt_logs('raw_securities','raw_temp_securities')"
                ] 
            )}}


with st_securities as(
    select * from {{ref('raw_temp_securities')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
 

raw_securities as (
    select security_id ,security_name ,security_type ,quantity , market_value, purchase_date , purchase_price , purchase_value,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted ,
    security_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(security_id as string),'!@|@!'),'~#~', 
     ifnull(cast(security_name as string),'!@|@!'),'~#~', 
     ifnull(cast(security_type as string),'!@|@!'),'~#~',
     ifnull(cast(quantity as string),'!@|@!'),'~#~',
     ifnull(cast(market_value as string),'!@|@!'),'~#~',
     ifnull(cast(purchase_date as string),'!@|@!'),'~#~',
     ifnull(cast(purchase_price as string),'!@|@!'),'~#~',
     ifnull(cast(purchase_value as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_securities , elt_logs el
),

final as (
    select * from raw_securities
)

select * from final
