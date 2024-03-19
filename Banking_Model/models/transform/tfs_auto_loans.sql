{{config(materialized='table')}}


with auto_loan_details as (
     select loan_id , loan_type_id, account_number , sanction_date ,loan_amount , tenure , transfer_id
     from {{ref('raw_loan_details')}} 
    where loan_type_id BETWEEN 17 AND 24
),
elt_logs as (
    select max(elt_run_id) as elt_run_id from raw_elt_logs
),

repossession_info as (
     select  rla.loan_id, ald.loan_type_id,ald.sanction_date ,ald.account_number ,ald.loan_amount ,ald.tenure,emi_amount, ald.transfer_id,  first_emi_date,  last_emi_paid_on , month_pass_due_date , amount_pass_due_date
     ,  Next_EMI_Due_Date
     ,  Defaulter 
     , case 
           when Defaulter = 1 then 
                case
                     when DATEDIFF(days, last_emi_paid_on, GETDATE()) > 90 then DATEADD(month, 3,last_emi_paid_on)
                end
        else null   
        end as Repossession_Begin
     , case 
           when Repossession_Begin is not null then 
               case 
                   when DATEDIFF(days, Repossession_Begin , GETDATE() ) <= 15 then DATEDIFF(days, Repossession_Begin , GETDATE() )  
                   else 0 
               end   
        else null       
        end as 
       Repossession_days_left
     , case when Repossession_days_left = 0 then 1 else 0 end as Repossessed 
     
     from {{ref('tfs_loan_accounts')}}  rla
     join   auto_loan_details ald 
     on ald.loan_id = rla.loan_id
   
),

elt_columns as (
 select r.* , 
    GETDATE() as _elt_inserted , 
    r.loan_id as _elt_joinkey ,
    sha2(concat(ifnull(cast(loan_id as string), '!@|@!'), '~#~',
    ifnull(cast(emi_amount as string), '!@|@!'), '~#~',
    ifnull(cast(first_emi_date as string), '!@|@!'), '~#~',
    ifnull(cast(last_emi_paid_on as string), '!@|@!'), '~#~',
    ifnull(cast(next_emi_due_date as string), '!@|@!'), '~#~',
    ifnull(cast(defaulter as string), '!@|@!'), '~#~',
    ifnull(cast(Repossession_Begin as string), '!@|@!'), '~#~',
    ifnull(cast(Repossession_days_left as string), '!@|@!'), '~#~',
    ifnull(cast(Repossessed as string), '!@|@!'), '~#~',
    ifnull(cast(month_pass_due_date as string), '!@|@!'), '~#~',
    ifnull(cast(amount_pass_due_date as string), '!@|@!'), '~#~'  ),256)  as _elt_hashkey
    from repossession_info as r , elt_logs el
),

final as (
    select * from elt_columns
)

select * from final