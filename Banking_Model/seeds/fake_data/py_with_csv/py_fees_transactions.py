import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_fees_transactions.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['transaction_id','account_id', 'format_id', 'transaction_type_id', 'source',
               'filename','sender', 'receiver', 'transaction_date', 'process_date',
               'process_status', 'created_by', 'created_date', 'updated_date',
               'updated_by', 'currency_code', 'check_number','transaction_amount', 
               'bank_to_bank_info', 'ach_addenda']
    writer.writerow(header)
    for i in range(131910, 155000):
        account_id = random.randint(1, 33343)
        format_id = random.choice([1,7])
        if(format_id == 7):
            transaction_type_id = random.choice([6,3,2,1,5,10,8,14,13,11,15,16,17,18,19])
        else :   
            transaction_type_id = 20
        source = random.choice(['FIRD', 'BAI'])
        filename = fake.file_name() + "_" + fake.date()
        sender = fake.first_name()
        receiver = 'Bank Services Fees'
        transaction_date = fake.date_between(start_date='-4000d', end_date='today')
        process_date = transaction_date + timedelta(days=1)
        process_status = 'Successful'
        created_by = 'Fees Transaction'
        created_date = transaction_date
        updated_date = process_date
        updated_by = 'Fees Transaction'
        currency_code = 'US$'
        check_number = fake.aba()
        transaction_amount = fake.pyfloat(
            positive=True, left_digits=5, right_digits=2, min_value=1.0)
        bank_to_bank_info = fake.file_name()
        ach_addenda = sender+'_'+process_status+'_'+receiver
    
        row = [i, account_id, format_id, transaction_type_id, source, filename,
               sender, receiver, transaction_date, process_date,
               process_status, created_by, created_date, updated_date,
               updated_by, currency_code, check_number , transaction_amount, bank_to_bank_info, ach_addenda]

        writer.writerow(row)


file.close()



