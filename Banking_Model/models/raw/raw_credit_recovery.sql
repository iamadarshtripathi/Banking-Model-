{{config(tags=["p3"],post_hook=[
                "UPDATE raw.raw_credit_recovery rcr SET rcr._ELT_Updated = GETDATE(), rcr.credit_index = st.credit_index , rcr.scheduled_calls = st.scheduled_calls , rcr.scheduled_emails = st.scheduled_emails ,  rcr.interest = st.interest ,
                 rcr.paid = st.paid , rcr.due = st.due ,
                 rcr.balance = st.balance , rcr.date = st.date , rcr.city = st.city
                 FROM raw.raw_temp_credit_recovery st 
                 WHERE rcr.credit_index = st.credit_index
                 AND st.METADATA$ACTION = 'INSERT' and st.METADATA$ISUPDATE = True" ,
                
                "UPDATE raw.raw_credit_recovery rcr SET rcr._ELT_Is_Deleted = 1 
                 FROM raw.raw_temp_credit_recovery st 
                 where rcr.credit_index = st.credit_index 
                 and st.METADATA$ACTION = 'DELETE' and  st.METADATA$ISUPDATE = False",

                 "call raw.sp_Update_elt_logs('raw_credit_recovery','raw_temp_credit_recovery')"
               ] 
            )}}

with st_credit_recovery as (
     select * from {{ref('raw_temp_credit_recovery')}}  where METADATA$ACTION = 'INSERT' and METADATA$ISUPDATE = 0
),

elt_logs as (
    select max(elt_run_id) as elt_run_id from {{ref('raw_elt_logs')}} 
),

raw_credit_recovery as  (
    select credit_index , scheduled_calls , scheduled_emails , interest , paid , due , balance , date ,city , 
    CURRENT_TIMESTAMP() as _elt_inserted ,
    cast('12/31/9999' as date)  as _elt_updated ,
    0 as _elt_is_deleted,
    credit_index as _elt_joinkey,
     sha2(concat(ifnull(cast(credit_index as string),'!@|@!'),'~#~', 
     ifnull(cast(scheduled_calls as string),'!@|@!'),'~#~', 
     ifnull(cast(scheduled_emails as string),'!@|@!'),'~#~', 
     ifnull(cast(interest as string),'!@|@!'),'~#~',
     ifnull(cast(paid as string),'!@|@!'),'~#~',
     ifnull(cast(due as string),'!@|@!'),'~#~',
     ifnull(cast(balance as string),'!@|@!'),'~#~',
     ifnull(cast(date as string),'!@|@!'),'~#~',
     ifnull(cast(city as string),'!@|@!'),'~#~'), 256) as _elt_hashkey,
    el.elt_run_id as _elt_run_id
    from st_credit_recovery, elt_logs el
),

final as (
    select * from raw_credit_recovery
)

select * from final 
