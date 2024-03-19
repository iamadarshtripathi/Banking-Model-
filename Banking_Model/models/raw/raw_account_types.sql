-- config to make base table account_type not appendable.
{{ config(tags = ["p3"]) }} {{ config(
    post_hook = [
                "UPDATE raw.raw_account_types rc SET rc._ELT_Updated = GETDATE() ,rc.account_type_id = st.account_type_id , rc.account_type = st.account_type ,
                 rc.account_type_description = st.account_type_description
                    FROM raw.raw_temp_account_types st 
                    WHERE rc.account_type_id = st.account_type_id
                    AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                "UPDATE raw.raw_account_types rc SET rc._ELT_Is_Deleted = 1 
                    FROM raw.raw_temp_account_types st 
                    where rc.account_type_id = st.account_type_id 
                    and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                "call raw.sp_Update_elt_logs('raw_account_types','raw_temp_account_types')"
                
               ]
) }} -- source for accessing staging account_types table data.
with st_account_types as(
    select
        *
    from
        {{ ref('raw_temp_account_types') }}
    where
        METADATA$ACTION = 'INSERT'
        and METADATA$ISUPDATE = False
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ ref('raw_elt_logs') }}
),
raw_account_types as (
    select
        account_type_id,
        account_type,
        account_type_description,
        GETDATE() as _elt_inserted,
        cast('12/31/9999' as date) as _elt_updated,
        0 as _elt_is_deleted,
        account_type_id as _elt_joinkey,
        SHA2(
            CONCAT(
                IFNULL(CAST(ACCOUNT_TYPE_ID AS STRING), '!@|@!'),
                '~#~',
                IFNULL(CAST(ACCOUNT_TYPE AS STRING), '!@|@!'),
                '~#~',
                IFNULL(
                    CAST(ACCOUNT_TYPE_DESCRIPTION AS STRING),
                    '!@|@!'
                ),
                '~#~'
            ),
            256
        ) as _ELT_HASHKEY,
        'faker' as _elt_source_system,
        el.elt_run_id as _elt_run_id
    from
        st_account_types,
        elt_logs el
),
final as (
    select * from raw_account_types
)
select * from final