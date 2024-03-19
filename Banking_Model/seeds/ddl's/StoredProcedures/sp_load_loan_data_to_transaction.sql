CREATE OR REPLACE PROCEDURE sp_load_loan_data_to_transaction()
  RETURNS VARCHAR
  LANGUAGE SQL
AS
$$
DECLARE
    int_loan_id NUMBER(38,0);
    int_account_number NUMBER(38,0);
    var_count varchar;
    int_counter NUMBER(38,0);
    loan_cursor CURSOR FOR select loan_id, account_number from staging.loan_details ;
    BEGIN
        --OPEN loan_cursor;
        --FETCH loan_cursor into int_loan_id, int_account_number;
        For record in loan_cursor do
            int_loan_id := record.loan_id;
            int_account_number := record.account_number;
            int_counter := 1;
            WHILE (int_counter <= 3) DO
                    insert into staging.transactions(
                        transaction_id,account_id,loan_id,format_id,transaction_type_id,
                        source, filename, sender, receiver, transaction_date, process_date,
                        process_Status,created_by,created_date, updated_date, updated_by, 
                        currency_code, transaction_amount,loan_type_id)
                        (
                            Select SEQ_TRANSACTION.nextval, l.account_number,l.loan_id, 5, 8, 'BAI',
                            CONCAT('loan-emi_',GETDATE()), c.customer_name, 'bank emi', DATEADD(month, :int_counter, l.sanction_date), 
                            DATEADD(month, 1, l.sanction_date), 'Successful', 'Automated Transaction', GETDATE(), GETDATE(),
                            'Automated Transaction', 'US$', l.emi_amount, l.loan_type_id from staging.loan_details l
                            JOIN staging.accounts a on l.account_number = a.account_id 
                            JOIN staging.customers c on a.customer_id = c.customer_id 
                            where l.loan_id = :int_loan_id  and l.account_number = :int_account_number
                        ); 
                   int_counter := int_counter + 1;
              END WHILE;
        END FOR;
    --CLOSE loan_cursor;
    --RETURN var_count;
END;
$$;

--call sp_load_loan_data_to_transaction();

CREATE SEQUENCE SEQ_TRANSACTION START = 1001 INCREMENT = 1;