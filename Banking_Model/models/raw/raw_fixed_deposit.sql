{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_fixed_deposit r SET r._ELT_Updated = GETDATE() ,r.customer_id = st.customer_id , r.FD_ID = st.FD_ID ,
                 r.AMOUNT = st.AMOUNT , r.AUTOMATIC_RENEWAL = st.AUTOMATIC_RENEWAL ,
                 r.CERTIFICATE_NUMBER = st.CERTIFICATE_NUMBER , r.FROM_DATE = st.FROM_DATE , r.INTEREST_RATE = st.INTEREST_RATE, r.ISSUED_DATE = st.ISSUED_DATE ,
                 r.PAYMENT_METHOD = st.PAYMENT_METHOD , r.Period = st.Period , r.RECEIPT_NUMBER = st.RECEIPT_NUMBER , r.TO_DATE = st.TO_DATE
                 FROM raw.raw_temp_fixed_deposit st 
                 WHERE r.customer_id = st.customer_id
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                  
                "UPDATE raw.raw_fixed_deposit r SET r._ELT_Is_Deleted = 1 
                FROM raw.raw_temp_fixed_deposit st 
                where r.customer_id = st.customer_id 
                and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                "call raw.sp_Update_elt_logs('raw_fixed_deposit','raw_temp_fixed_deposit')"
                ] 
            )}}


with st_fixed_deposit as(
    select * from {{ref('raw_temp_fixed_deposit')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
 

raw_check_book as (
    select CUSTOMER_ID , FD_ID , AMOUNT , AUTOMATIC_RENEWAL ,
    CERTIFICATE_NUMBER , FROM_DATE , INTEREST_RATE , ISSUED_DATE , 
    PAYMENT_METHOD , PERIOD , RECEIPT_NUMBER , TO_DATE ,
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated ,
    0 as _elt_is_deleted ,
    CONCAT(CUSTOMER_ID,'-',FD_ID) as _elt_joinkey ,
    sha2(concat(ifnull(cast(CUSTOMER_ID as string),'!@|@!'),'~#~', 
     ifnull(cast(FD_ID as string),'!@|@!'),'~#~', 
     ifnull(cast(AMOUNT as string),'!@|@!'),'~#~',
     ifnull(cast(AUTOMATIC_RENEWAL as string),'!@|@!'),'~#~',
     ifnull(cast(CERTIFICATE_NUMBER as string),'!@|@!'),'~#~',
     ifnull(cast(FROM_DATE as string),'!@|@!'),'~#~',
     ifnull(cast(INTEREST_RATE as string),'!@|@!'),'~#~',
     ifnull(cast(ISSUED_DATE as string),'!@|@!'),'~#~',
     ifnull(cast(PAYMENT_METHOD as string),'!@|@!'),'~#~',
     ifnull(cast(PERIOD as string),'!@|@!'),'~#~',
     ifnull(cast(RECEIPT_NUMBER as string),'!@|@!'),'~#~',
     ifnull(cast(TO_DATE as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_fixed_deposit , elt_logs el
),

final as (
    select * from raw_check_book
)

select * from final

