
-- CREATE OR REPLACE PROCEDURE sp_add_fk_raw_loan_details()
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- AS
-- $$
-- BEGIN

-- -- Add the foreign key constraint
--     -- ALTER TABLE RAW.RAW_LOAN_DETAILS
--     -- ADD FOREIGN KEY (EMPLOYEE_ID)
--     -- REFERENCES RAW.RAW_EMPLOYEES (EMPLOYEE_ID)
--     -- INITIALLY DEFERRED;

    
--     -- Update the foreign key column based on the condition
--     UPDATE RAW.RAW_LOAN_DETAILS
--     SET EMPLOYEE_ID = CASE
--         WHEN LOAN_ID % 2 = 0 THEN 2 
--         WHEN LOAN_ID % 3 = 0 THEN 3 
--         WHEN LOAN_ID % 5 = 0 THEN 5 
--         WHEN LOAN_ID % 7 = 0 THEN 7 
--         WHEN LOAN_ID % 11 = 0 THEN 11 
--         WHEN LOAN_ID % 13 = 0 THEN 13 
--         WHEN LOAN_ID % 17 = 0 THEN 17 
--         WHEN LOAN_ID % 19 = 0 THEN 19 
--         WHEN LOAN_ID % 23 = 0 THEN 23 
--         WHEN LOAN_ID % 29 = 0 THEN 29 
--         ELSE 1 
--         END;
    
--     RETURN 'Foreign key added successfully.';
-- END;
-- $$;



-- call sp_add_fk_raw_loan_details();