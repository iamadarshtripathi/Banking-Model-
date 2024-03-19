with auto_loans as (
   select loan_id , sanction_date , loan_amount ,loan_type_id , emi_amount,defaulter  , repossessed , account_number , transfer_id from {{ref('tfs_auto_loans')}}
),

interest_rate as (
select al.* , rlt.interest_rate from auto_loans al 
join {{ref('tfs_loan_types')}}  rlt
on al.loan_type_id = rlt.loan_type_id

),

auto_info as (
 select i.* , rai.auto_id , rai.brand , rai.model ,
 rai.price from interest_rate as i
 join {{ref('raw_map_auto_loan')}} as mal on i.loan_id = mal.loan_id 
 join {{ref('raw_auto_info')}}  as rai on mal.auto_id = rai.auto_id
) ,

customer_income_bank_branch as (
 select ai.*, rc.monthly_income as customer_monthly_income , rb.branch_state as branch_state  
 from auto_info as ai 
 join {{ref('raw_accounts')}}  as ra 
  on ai.account_number = ra.account_id
 join {{ref('raw_branches')}}  as rb 
  on rb.branch_id = ra.branch_id
 join {{ref('raw_customers')}} as rc 
  on rc.customer_id = ra.customer_id 
 ),

 transfer_type as (
  select cibb.* , tld.transfer_type from customer_income_bank_branch cibb
  left join {{ref('raw_transfer_loan_data')}}  tld 
  on cibb.transfer_id = tld.transfer_id  
) ,

max_amount_paid as (
    select loan_id, sum(emi_amount) as amount_paid from {{ref('raw_loan_transactions')}}  
    group by loan_id    
    
    
),
map_max_amount_paid as (
  select tt.*, map.amount_paid from transfer_type tt
  join max_amount_paid map
  on tt.loan_id = map.loan_id
),
loss as (
select mmap.* , case when mmap.defaulter = 1 then mmap.amount_paid else null end as loss from map_max_amount_paid as mmap
),

final as (
select * from loss 
)

select * from final 