-- source for accessing staging accounts table data.customers
{{config(
    tags = ["p3"],
    post_hook = [
                "UPDATE raw.raw_accounts ra SET ra._ELT_Updated = GETDATE() ,ra.account_id = st.account_id , ra.account_number = st.account_number ,
                 ra.account_close_date = st.account_close_date , ra.account_open_date = st.account_open_date ,
                 ra.account_status = st.account_status , ra.account_type_id = st.account_type_id , ra.created_by = st.created_by, ra.created_date = st.created_date , 
                 ra.customer_id = st.customer_id , ra.branch_id = st.branch_id , ra.updated_by = st.updated_by , ra.updated_date = st.updated_date,
                 ra.account_application_id = st.account_application_id
                    FROM raw.raw_temp_accounts st  
                    WHERE ra.account_id = st.account_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_accounts ra SET ra._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_accounts st 
                    where ra.account_id = st.account_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                "call raw.sp_Update_elt_logs('raw_accounts','raw_temp_accounts')"
                ]
)}} 

with st_accounts as(
    select * from {{ref('raw_temp_accounts')}} where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = False
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}}
),
raw_accounts as (
    select
        account_id,
        customer_id,
        account_type_id,
        branch_id,
        account_number,
        account_open_date,
        account_close_date,
        account_status,
        created_by,
        created_date,
        updated_date,
        updated_by,
        account_application_id,
        GETDATE() as _elt_inserted,
        cast('12/31/9999' as date) as _elt_updated,
        False as _elt_is_deleted,
        account_id as _elt_joinkey,
        SHA2(
            CONCAT(
                IFNULL(CAST(ACCOUNT_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_CLOSE_DATE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_NUMBER AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_OPEN_DATE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_STATUS AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_TYPE_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(CREATED_BY AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(CREATED_DATE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(CUSTOMER_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(UPDATED_BY AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(UPDATED_DATE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_APPLICATION_ID AS STRING), '!@|@!'),
                '~#~'
            ),
            256
        ) as _ELT_HASHKEY,
        'faker' as _elt_source_system,
        el.elt_run_id as _elt_run_id
    from
        st_accounts,
        elt_logs el
),
final as (
    select * from raw_accounts
)
select * from final