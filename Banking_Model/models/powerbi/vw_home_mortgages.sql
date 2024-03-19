-- This is view is created to get the details of overall mortgages
-- like mortgage approval rate, average loan volume, average interest rate, origination volume and
-- other details that need to create report


WITH raw_loan_applications AS (
    SELECT * FROM {{ref('raw_loan_applications')}} where loan_type_id between 9 and 16
),
loan_details_types AS (
    SELECT *,
        CASE 
            WHEN tfs_loan_types.loan_type_id = raw_loan_details.loan_type_id THEN 
                  tfs_loan_types.interest_rate
            ELSE NULL 
        END AS interest_rates
    FROM {{ref('raw_loan_details')}}  
    LEFT JOIN {{ref('tfs_loan_types')}} ON raw_loan_details.loan_type_id = tfs_loan_types.loan_type_id
    WHERE raw_loan_details.loan_type_id BETWEEN 9 AND 16   
),
loan_columns AS (
    SELECT 
        la.APPLICATION_ID,
        la.APPLIED_DATE,
        la.EMPLOYEE_ID,
        CASE
            WHEN la.APPLICATION_ID = ldt.APPLICATION_ID THEN ldt.loan_id
            ELSE NULL
        END AS loan_id,
        CASE
            WHEN la.APPLICATION_ID = ldt.APPLICATION_ID THEN ldt.loan_amount
            ELSE NULL
        END AS loan_amount,
        CASE
            WHEN la.APPLICATION_ID = ldt.APPLICATION_ID THEN ldt.sanction_date
            ELSE NULL
        END AS sanction_date,
        ldt.interest_rate
    FROM raw_loan_applications as la
    LEFT JOIN loan_details_types as ldt
        ON la.APPLICATION_ID = ldt.APPLICATION_ID
),

raw_employee as(
   SELECT loan_columns.* ,
          re.branch_id
   FROM loan_columns
   left join {{ref('raw_employees')}} as re
   on loan_columns.employee_id = re.employee_id
),

raw_branch as(
  select raw_employee.*,
         rb.branch_city,
         rb.branch_state
  from raw_employee
  left join {{ref('raw_branches')}} as rb 
  on raw_employee.branch_id = rb.branch_id
),

final as(
  select raw_branch.*,
         sat.region
  from raw_branch 
  left join {{source('REF','STATESABBREVATIONS')}} as sat
  on raw_branch.branch_state = sat.state
)

SELECT * FROM final