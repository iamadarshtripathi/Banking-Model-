CREATE OR REPLACE PROCEDURE raw.sp_check_and_drop_table(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    DECLARE 
        current_run_id NUMBER(38,0);
    BEGIN
      SELECT MAX(elt_run_id) INTO current_run_id FROM raw.raw_elt_logs;
      
      IF (current_run_id != 1) THEN 
          EXECUTE IMMEDIATE 'DROP TABLE ' || TABLE_NAME;
      END IF;
     
      RETURN 'Table dropped successfully';
    END;
$$;


--call sp_check_and_drop_table('raw_temp_transaction');