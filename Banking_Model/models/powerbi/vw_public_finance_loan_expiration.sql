

with expiration_table as (
    select * from {{ref('tfs_loan_expiration')}}
)
 select * from expiration_table
    where datediff(day,EXPECTED_CLOSE_DATE,ACTUAL_CLOSE_DATE)<-1