/*
 * This stored proc insert data in ELT logs table. This would help identifying inserts, updates and delete count for each model.
*/

CREATE OR REPLACE PROCEDURE sp_Update_elt_logs (model_name varchar(100) , model_temp_table varchar(100))
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
DECLARE
    int_elt_run_id  INT;
    int_insert_count number;
    int_update_count number;
    int_delete_count number;
    var_temp_query varchar(500);
       BEGIN
           var_temp_query := 'SELECT COUNT(*) FROM ' || :model_temp_table || ' WHERE METADATA$ACTION = \'INSERT\' AND METADATA$ISUPDATE = FALSE';
           EXECUTE IMMEDIATE var_temp_query ;
           select $1 into int_insert_count from table(result_scan(last_query_id()));
            
           var_temp_query := 'SELECT COUNT(*) FROM ' || :model_temp_table || ' WHERE METADATA$ACTION = \'INSERT\' AND METADATA$ISUPDATE = TRUE';
           execute immediate :var_temp_query;
           select $1 into int_update_count from table(result_scan(last_query_id()));

           var_temp_query := 'SELECT COUNT(*) FROM ' || :model_temp_table || ' WHERE METADATA$ACTION = \'DELETE\' AND METADATA$ISUPDATE = FALSE';
           execute immediate :var_temp_query ;
           select $1 into int_delete_count from table(result_scan(last_query_id()));
           

           select max(elt_run_id) into int_elt_run_id from raw_elt_logs where ELT_RUN_STATUS = 'ELT TRIGGERED';

           INSERT INTO raw.raw_elt_logs (ELT_RUN_ID, ELT_RUN_STATUS, ELT_RUN_DATE, ELT_MODEL, ELT_INSERT, ELT_UPDATE, ELT_DELETE)
               values (:int_elt_run_id, 'Model Triggered', GETDATE(), :model_name, :int_insert_count, :int_update_count, :int_delete_count);
       

       END;
$$;

--call raw.sp_Update_elt_logs('raw_check_book','raw_temp_check_book');