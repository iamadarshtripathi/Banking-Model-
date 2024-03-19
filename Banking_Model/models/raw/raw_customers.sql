-- will take all the columns from stream while updating once the post hook works successfully
-- second query will set flag on all the deleted rows
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_customers  rc SET rc._elt_updated = GETDATE() ,rc.customer_id = st.customer_id , rc.customer_name = st.customer_name ,
                 rc.customer_address = st.customer_address , rc.customer_address_line2 = st.customer_address_line2 ,
                 rc.customer_city = st.customer_city , rc.customer_state = st.customer_state , rc.customer_zip = st.customer_zip, rc.customer_phone = st.customer_phone , 
                 rc.customer_type = st.customer_type , rc.customer_taxpayer_id = st.customer_taxpayer_id , rc.cif_number = st.cif_number , rc.created_by = st.created_by , rc.updated_by = st.updated_by ,
                 rc.created_date = st.created_date , rc.updated_date = st.updated_date ,rc.ssn = st.ssn , rc.email = st.email , rc.monthly_income = st.monthly_income ,
                 rc.net_asset_value = st.net_asset_value , rc.net_liability = st.net_liability 
                    FROM raw.raw_temp_customers st 
                    WHERE rc.customer_id = st.customer_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                    
                "UPDATE raw.raw_customers rc SET rc._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_customers st 
                    where rc.customer_id = st.customer_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False" ,

                    "call raw.sp_Update_elt_logs('raw_customers','raw_temp_customers')"
               ] 
            )}}

with st_customers as(
    select * from {{ref('raw_temp_customers')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 
raw_customers as (
     select customer_id , customer_name , customer_address , customer_address_line2 , customer_city , customer_state ,
    customer_zip, customer_phone , customer_type , customer_taxpayer_id , cif_number , created_by , updated_by , created_date , updated_date ,
    ssn , email , monthly_income , net_asset_value , net_liability , 
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated,
    false as _elt_is_deleted,
    customer_id as _elt_joinkey,
    SHA2(CONCAT(IFNULL(CAST(CUSTOMER_ID AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(CUSTOMER_NAME AS STRING),'!@|@!'),'~#~', 
    IFNULL(CAST(CUSTOMER_ADDRESS AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_ADDRESS_LINE2 AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_CITY AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_STATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_ZIP AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_PHONE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_TYPE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_TAXPAYER_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CIF_NUMBER AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CREATED_BY AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(UPDATED_BY AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CREATED_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(UPDATED_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(SSN AS STRING), '!@|@!'),'~#~',
    IFNULL(CAST(EMAIL AS STRING), '!@|@!'),'~#~',
    IFNULL(CAST(MONTHLY_INCOME AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(NET_ASSET_VALUE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(NET_LIABILITY AS STRING),'!@|@!'),'~#~'),256)  AS _ELT_HASHKEY,  
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_customers, elt_logs el
    
    
),

final as (
    select * from raw_customers
)

select * from final