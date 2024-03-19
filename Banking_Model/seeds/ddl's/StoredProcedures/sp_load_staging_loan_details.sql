CREATE OR REPLACE PROCEDURE sp_load_staging_loan_details()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
    BEGIN
        
        insert into staging.loan_details(
            loan_id,account_number,sanction_date,loan_amount,loan_type_id,tenure,emi_amount
            )
               (
                select t.loan_id , t.account_id , t.transaction_date , t.transaction_amount , lt.loan_type_id , lt.time_duration ,
                    ROUND((t.transaction_amount * (lt.interest_rate/1200) * POWER((1 + (lt.interest_rate/1200)), lt.time_duration))/ (POWER((1 + (lt.interest_rate/1200)), lt.time_duration) - 1),2) 
                    from staging.transactions as t inner join staging.loan_types as lt on t.loan_type_id = lt.loan_type_id
                        ); 

    END;
$$;

--call sp_load_staging_loan_details;

