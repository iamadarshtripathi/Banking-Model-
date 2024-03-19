-- This is view is created to get the details of overall mortgages
-- like refinancing volume, foreclosure rate, delinquency rate, customer satisfaction and
-- other details that need to create report


with loan_details_transaction as(
    select rld.loan_id , 
           rld.loan_type_id , 
           rld.application_id ,            
           rld.loan_amount , 
           rld.sanction_date , 
           rlt.account_id, 
           rlt.emi_amount , 
           rlt.transaction_date 
     from {{ref('raw_loan_transactions')}} as rlt
     join {{ref('raw_loan_details')}} as rld
     on rlt.loan_id = rld.loan_id
      where rld.loan_type_id BETWEEN 9 AND 16
), 

approved_loan_details as(
     select ldt.loan_id,
            ldt.loan_type_id, 
            ldt.application_id,
            ldt.account_id,
            ldt.loan_amount ,  
            ldt.sanction_date ,
            ldt.emi_amount , 
            ldt.transaction_date ,             
            la.customer_id, 
            la.property_id,
            la.applied_date
            from loan_details_transaction ldt 
            join {{ref('raw_loan_applications')}} la
            on ldt.application_id = la.application_id 
),

defaulter_foreclosure as (
     select loan_id,
     loan_type_id ,
     loan_amount ,
     sanction_date,
     emi_amount,
     applied_date,     
     property_id , 
     min(transaction_date) as First_EMI_DATE, 
        max(transaction_date) as Last_EMI_Paid_On, 
        case 
            when DATEDIFF(days, max(transaction_date), GETDATE()) > 30 then null else DATEADD(month, 1, max(transaction_date)) 
        end as Next_EMI_Due_Date, 
        case 
            when DATEDIFF(days, max(transaction_date), GETDATE()) > 30 then 1 else 0 
        end as Defaulter 
     , case 
           when Defaulter = 1 then 
                case
                     when DATEDIFF(days, max(transaction_date), GETDATE()) > 90 then DATEADD(month, 3, max(transaction_date)) 
                     else null
                end
        else null   
        end as Foreclosure_Begin
     , case 
           when Foreclosure_Begin is not null then 
               case 
                   when DATEDIFF(days, Foreclosure_Begin , GETDATE() ) <= 15 then DATEDIFF(days, Foreclosure_Begin , GETDATE() )  
                   else 0 
               end   
        else null       
        end as 
       Foreclosure_days_left
     , case when Foreclosure_days_left = 0 then 1 else 0 end as Foreclosured
     from approved_loan_details 
     
     where emi_amount is NOT null
     group by loan_id, emi_amount , loan_type_id ,
     property_id,
     loan_amount,
     sanction_date,applied_date
),

mortgages_loan as(
     select df.loan_id , df.loan_type_id , df.First_EMI_DATE , df.Last_EMI_Paid_On ,
     df.Next_EMI_Due_Date , df.Defaulter ,df.Foreclosure_Begin , df.Foreclosure_days_left ,df.Foreclosured,
       df.emi_amount ,  df.loan_amount , df.sanction_date, df.applied_date
       from defaulter_foreclosure df join {{ref('raw_mortgages_info')}} mi
       on mi.property_id = df.property_id 
),

refinancing_loan_data as
(
     select ml.* ,   
     case 
         when CURRENT_DATE() > DATEADD(YEAR, 5, ml.sanction_date) then DATEADD(YEAR, 5, ml.sanction_date)
         else null
         end AS refinancing_date ,
    case
      when refinancing_date is not null then rl.refinancing_type 
      else null
      end as refinancing_type,
     case
          when refinancing_date is not null then rl.refinancing_loan_amount
          else null
          end as refinancing_loan_amount
     from mortgages_loan ml left join {{ref('raw_refinancing_loan')}} rl
     on ml.loan_id = rl.loan_id
),


dashboard_data as (
select rld.* ,  cs.satisfaction_score 
from refinancing_loan_data rld 
left join {{ref('raw_customer_satisfaction')}} cs 
on rld.loan_id = cs.loan_id 
) 

select * from dashboard_data