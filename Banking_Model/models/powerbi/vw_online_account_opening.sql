--------------------------------------------------

-- This view is genrated to get the details of application that 
-- are completed,incompleted or declined on the basis of applied date 


with raw_account_application as(
   select 
         account_application_id,
         application_status,
         applied_date,
         account_application_state
   from {{ref('raw_account_applications')}} 
),

account_applications_accounts as(
   select 
         raa.account_application_id,
         raa.application_status,
         raa.applied_date,
         raa.account_application_state,
         ra.account_id,
         ra.branch_id
   from raw_account_application as raa
   left join {{ref('raw_accounts')}} ra 
       on raa.account_application_id = ra.account_application_id 
),

account_opening as(
   select 
         aaa.*,
         rb.branch_state
   from account_applications_accounts as aaa
   left join {{ref('raw_branches')}} as rb
        on aaa.branch_id = rb.branch_id
),

final as(
  select ao.*,
         sat.region
  from account_opening as ao
  left join {{source('REF','STATESABBREVATIONS')}} as sat
  on ao.branch_state = sat.state
)
select * from final