import csv
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dateutil import parser

# Specify the path to your CSV file
csv_read_file_path = 'staging.csv'
csv_write_file_path = 'load_emi_transactions.csv'
current_Date = date.today()
with open(csv_write_file_path, 'w', newline='') as writefile:

    writer = csv.writer(writefile)

    header = ['transaction_counter', 'account_number', 'loan_id', 'format_id', 'transaction_type_id', 'source', 'filename',
                        'customer_name', 'receiver', 'transaction_date', 'process_Date', 'process_Status', 
                        'created_by', 'created_date', 'updated_date', 'updated_by', 'currency_code', 'emi_amount',
                        'loan_type_id']
    writer.writerow(header)

    # Open the CSV file
    with open(csv_read_file_path, 'r') as readfile:
        # Create a CSV reader object
        csv_reader = csv.reader(readfile)
        int_transactionId_counter = 100000
        # Read and process each row of the CSV file
        for row in csv_reader:
            int_loan_id = row[1]
            int_account_number = row[0]
            str_customer_name = row[2]
            dt_sanction_date = row[3]
            int_emi_amount = row[4]
            int_loan_type_id = row[5]
            int_counter = 1
            while int_counter <= 5:
                transaction_counter = int_transactionId_counter
                account_number = int_account_number
                loan_id = int_loan_id
                format_id = 5
                transaction_type_id = 8
                source = 'BAI'
                filename = 'loan-emi_' + current_Date.strftime('%Y-%m-%d')
                customer_name = str_customer_name
                receiver = 'bank emi'
                transaction_date = parser.parse(dt_sanction_date).date() + relativedelta(month=int_counter)
                #transaction_date = dt_sanction_date + relativedelta(month=int_counter)
                process_Date = transaction_date + timedelta(days=1)
                process_Status = 'Successful'
                created_by = 'Automated Transaction'
                created_date = transaction_date
                updated_date = process_Date
                updated_by = 'Automated Transaction'
                currency_code = 'US$'
                emi_amount = int_emi_amount
                loan_type_id = int_loan_type_id
                int_counter = int_counter + 1
                int_transactionId_counter = int_transactionId_counter + 1
                row = [transaction_counter, account_number, loan_id, format_id, transaction_type_id, source, filename,
                            customer_name, receiver, transaction_date, process_Date, process_Status, 
                            created_by, created_date, updated_date, updated_by, currency_code, emi_amount,
                            loan_type_id]
                
                writer.writerow(row)


readfile.close() 
writefile.close()   