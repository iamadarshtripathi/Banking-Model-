{{ config(
    tags = ["p3"],
    post_hook = [
                "UPDATE raw.raw_branches rb SET rb._ELT_Updated = GETDATE() ,rb.branch_id = st.branch_id , rb.branch_name = st.branch_name ,
                 rb.branch_address_line1 = st.branch_address_line1 , rb.branch_address_line2 = st.branch_address_line2 ,
                 rb.branch_city = st.branch_city , rb.branch_state = st.branch_state , rb.branch_zip = st.branch_zip, rb.branch_telephone = st.branch_telephone , 
                 rb.branch_code = st.branch_code , rb.branch_head_id = st.branch_head_id , rb.email_address = st.email_address 
                    FROM staging.st_branches st 
                    WHERE rb.branch_id = st.branch_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_branches rb SET rb._ELT_Is_Deleted = 1 
                    FROM staging.st_branches st 
                    where rb.branch_id = st.branch_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                "call raw.sp_Update_elt_logs('raw_branches','raw_temp_branches')"
               ]
)}} -- source for accessing staging branches table data.
with st_branches as(
    select * from {{ ref('raw_temp_branches') }}
    where
        METADATA$ACTION = 'INSERT'
        and METADATA$ISUPDATE = False
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ ref('raw_elt_logs') }}
),
raw_branches as (
    select
        branch_id,
        branch_head_id,
        branch_name,
        branch_address_line1,
        branch_address_line2,
        branch_city,
        branch_state,
        branch_zip,
        branch_telephone,
        email_address,
        branch_code,
        GETDATE() as _elt_inserted,
        cast('12/31/9999' as date) as _elt_updated,
        False as _elt_is_deleted,
        branch_id as _elt_joinkey,
        SHA2(
            CONCAT(
                IFNULL(CAST(BRANCH_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_HEAD_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_NAME AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_ADDRESS_LINE1 AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_ADDRESS_LINE2 AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_CITY AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_STATE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_ZIP AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_TELEPHONE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(EMAIL_ADDRESS AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(BRANCH_CODE AS STRING), '!@|@!'),
                '~#~'
            ),
            256
        ) as _ELT_HASHKEY,
        'faker' as _elt_source_system,
        el.elt_run_id as _elt_run_id
    from
        st_branches,
        elt_logs el
),
final as (
    select * from raw_branches
)
select * from final