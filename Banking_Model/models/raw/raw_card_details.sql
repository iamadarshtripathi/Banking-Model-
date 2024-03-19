-- source for accessing staging atm_card table data.
{{config(tags=["p3"], post_hook=[
                "UPDATE raw.raw_card_details rac SET rac._ELT_Updated = GETDATE() ,
                rac.card_id = st.card_id , rac.account_id = st.account_id ,rac.customer_id = st.customer_id,
                rac.card_type = st.card_type, rac.card_number = st.card_number, rac.card_limit = st.card_limit,
                rac.issued_date = st.issued_date, rac.valid_date = st.valid_date , rac.card_sub_type = st.card_sub_type
                    FROM staging.st_card_details st 
                    WHERE rac.card_id = st.card_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_card_details rac SET rac._ELT_Is_Deleted = 1 
                    FROM staging.st_card_details st 
                    where rac.card_id = st.card_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",
                    
                "call raw.sp_Update_elt_logs('raw_card_details','raw_temp_card_details')"
               ] 
            )}}
            
with st_card_details as(
    select * from {{ref('raw_temp_card_details')}}
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
), 

raw_card_details as (
    select card_id,account_id,customer_id , card_type,card_number ,card_limit, issued_date,valid_date,card_sub_type,
    case
     when (METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False) then CURRENT_TIMESTAMP()
    end as _elt_inserted ,
    cast('12/31/9999' as date) as _elt_updated,
    False as _elt_is_deleted,
    card_id as _elt_joinkey,
    SHA2(CONCAT(IFNULL(CAST(CARD_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(ACCOUNT_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CUSTOMER_ID AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CARD_TYPE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CARD_NUMBER AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CARD_LIMIT AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(ISSUED_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(VALID_DATE AS STRING),'!@|@!'),'~#~',
    IFNULL(CAST(CARD_SUB_TYPE AS STRING),'!@|@!'),'~#~'),256) as _ELT_HASHKEY,
    'faker' as _elt_source_system,
    el.elt_run_id as _elt_run_id
    from st_card_details, elt_logs el

),

final as (
    select * from raw_card_details
)

select * from final