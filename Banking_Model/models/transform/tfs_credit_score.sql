with account_defaulter_info as (
select sum(rla.defaulter) as defaulter_value , count(case when rla.defaulter = 0 then 1 END) as successful_loans , rld.account_number
from {{ref('tfs_loan_accounts')}} as rla 
inner join  {{source('RAW','RAW_LOAN_DETAILS')}} as rld 
on rla.loan_id = rld.loan_id
group by rld.account_number
),

customer_defaulter_info as (
select sum(adi.defaulter_value) as times_defaulter , sum(successful_loans) as times_successful , ra.customer_id from account_defaulter_info as adi
inner join  {{source('RAW','RAW_ACCOUNTS')}} as ra
on adi.account_number = ra.account_id
group by ra.customer_id
),


credit_score as (
select customer_id , 
       case 
           when (900 - (times_defaulter*100) + (times_successful*10)) < 300 then 300 else
           case  
               when (900 - (times_defaulter*100) + (times_successful*10)) >= 850 then 850 else (900 - (times_defaulter*100) + (times_successful*10))
            end
        end as credit_score from customer_defaulter_info
),

final as (
select * from credit_score
)

select * from final 