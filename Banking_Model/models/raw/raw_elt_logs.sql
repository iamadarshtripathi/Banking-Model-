-- source for accessing staging accounts table data.
{{
    config(tags=["p1"])
}}


with final as (
    select raw.elt_run_id_seq.nextval as elt_run_id
            , cast('ELT TRIGGERED' as varchar(23) )as elt_run_status
            , GETDATE() as elt_run_date
            , cast('' as varchar(200) ) as elt_model
            , cast(0 as number(38,0) ) as elt_insert
            , cast(0 as number(38,0) ) as elt_update
            , cast(0 as number(38,0) ) as elt_delete  from dual
)

select * from final