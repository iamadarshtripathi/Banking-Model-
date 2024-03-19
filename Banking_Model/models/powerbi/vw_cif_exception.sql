-- This is view is created to get the details of cif exceptions


with employee_branches as (
    select se.branch_id , se.employee_id , se.first_name , se.last_name  , rb.branch_name
    from {{ref('raw_employees')}} as se join {{ref('raw_branches')}} as rb 
    where se.branch_id = rb.branch_id
) ,


cif_exception as (
    select rce.EXCEPTION_ID , rce.CUSTOMER_ID , rce.EMPLOYEE_ID , eb.first_name as employee_name , 
    eb.branch_name , rce.OPEN_DATE,rce.REVIEW_DATE ,rce.DOC_STATUS ,rce.DESCRIPTION ,
    rce.COMMENTS  from
    {{ref('raw_cif_exception')}} as rce join employee_branches eb 
    where rce.employee_id = eb.employee_id
),

customer_name as(
    select ce.* , rc.customer_name , rc.cif_number from 
    {{ref('raw_customers')}} as rc join cif_exception ce
    where rc.customer_id = ce.customer_id
)

select * from customer_name
