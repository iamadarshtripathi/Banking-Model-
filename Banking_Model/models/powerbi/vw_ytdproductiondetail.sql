with ytd_table as (
    select 
    acc.account_id
    , acc.account_number
    , acc.account_open_date
    , acc.account_close_date
    , acc.account_status
    , CASE
      WHEN aty.account_type IN ('Savings Account', 'Money Market', 'Certificates of Deposits', 'Individual Retirement Accounts') THEN 'Interest-bearing account' 
      WHEN aty.account_type IN ('Checking Account', 'Brokerage Account') THEN 'Non-interest-bearing account'
      ELSE 'Other'
      END as account_type_group
    , brn.branch_id
    , brn.branch_name
    , brn.branch_code
    , brn.branch_city
    , brn.branch_state
    , CASE 
        WHEN brn.branch_state IN ('Alabama', 'Florida', 'Georgia', 'Mississippi', 'South Carolina', 'Tennessee') THEN 'Southeast'
        WHEN brn.branch_state IN ('Connecticut', 'Maine', 'Massachusetts', 'New Hampshire', 'Rhode Island', 'Vermont') THEN 'Northeast'
        WHEN brn.branch_state IN ('Illinois', 'Indiana', 'Michigan', 'Ohio', 'Wisconsin') THEN 'Midwest'
        WHEN brn.branch_state IN ('Arkansas', 'Louisiana', 'Oklahoma', 'Texas') THEN 'South Central'
        WHEN brn.branch_state IN ('Arizona', 'Colorado', 'New Mexico', 'Utah') THEN 'Southwest'
        WHEN brn.branch_state IN ('California', 'Hawaii', 'Nevada', 'Oregon', 'Washington') THEN 'West'
        ELSE 'Other'
        END AS branch_region
    , brn.branch_zip
    , tta.current_balance
    , tta.available_balance
    , tta.average_balance_mtd
from {{ref('tfs_transaction_accounts')}} tta
JOIN {{ref('raw_accounts')}} acc
     ON acc.account_id = tta.account_id
JOIN {{ref('raw_branches')}} brn
     ON acc.branch_id = brn.branch_id
LEFT OUTER JOIN {{ref('raw_account_types')}} aty
     ON acc.account_type_id = aty.account_type_id
WHERE acc.account_open_date >= DATE_TRUNC('YEAR', CURRENT_DATE()) --AS first_day_of_year
) 
select * from ytd_table