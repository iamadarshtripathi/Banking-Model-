CREATE OR REPLACE PROCEDURE sp_map_autoId_to_loanId()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
DECLARE 
    loan_id NUMBER(38,0);
    auto_id NUMBER(38,0);
    loan_id_cursor CURSOR for select loan_id from loan_details where loan_type_id between 17 and 24;
    BEGIN 
        OPEN loan_id_cursor;
            For record in loan_id_cursor do
                loan_id := record.loan_id;
                SELECT auto_id into :auto_id FROM auto_info QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) = 1;
                INSERT INTO MAP_AUTO_LOAN (auto_id , loan_id) 
                Values (:auto_id , :loan_id);
             
            END FOR; 
        CLOSE loan_id_cursor;
    END;

$$;