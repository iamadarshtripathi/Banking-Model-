import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'transactions.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['transaction_id','account_id', 'format_id', 'transaction_type_id', 'source',
               'filename','sender', 'receiver', 'transaction_date', 'process_date',
               'process_status', 'created_by', 'created_date', 'updated_date',
               'updated_by', 'currency_code', 'check_number','transaction_amount', 
               'bank_to_bank_info', 'foreign_transaction','sender_aba_routing_number'
               ,'receiver_aba_routing_number', 'beneficiary_name', 'originator_name', 
               'ach_addenda','loan_id', 'loan_type_id']
    writer.writerow(header)
    loan_id_value = 1
    for i in range(1, 100000 ):
        account_id = random.randint(1, 33343)
        transaction_type_id = random.randint(1, 15)
        format_id = random.randint(1,2)
        if (transaction_type_id == 8) :
         format_id = 1
        if (format_id == 2):
            format_id =  random.randint(2,7) 
        source = random.choice(['SWIFT MT', 'ACH', 'FIRD', 'BAI'])
        filename = fake.file_name() + "_" + fake.date()
        sender = fake.first_name()
        receiver = fake.first_name()
        transaction_date = fake.date_between(start_date='-2000d', end_date='today')
        process_date = fake.date_between(start_date='-2000d', end_date='today')
        process_status = random.choice(['Successful', 'Failed'])
        created_by = fake.name()
        created_date = fake.date_between(start_date='-2000d', end_date='today')
        updated_date = fake.date_between(start_date='-2000d', end_date='today')
        updated_by = fake.name()
        currency_code = fake.currency_code()
        check_number = fake.aba()
        transaction_amount = fake.pyfloat(
            positive=True, left_digits=5, right_digits=2, min_value=1.0)
        bank_to_bank_info = fake.file_name()
        foreign_transaction = fake.boolean()
        sender_aba_routing_number = fake.aba()
        receiver_aba_routing_number = fake.aba()
        beneficiary_name = fake.name()
        originator_name = fake.name()
        ach_addenda = sender+'_'+process_status+'_'+receiver
        if (transaction_type_id == 8):
            loan_id = loan_id_value
            loan_id_value = loan_id_value + 1
            employee_id = random.randint(1, 30000)
        else:
            loan_id = None
        if (transaction_type_id == 8):
            loan_type_id = random.randint(1, 40)
        else:
            loan_type_id = None
    
        row = [i, account_id, format_id, transaction_type_id, source, filename,
               sender, receiver, transaction_date, process_date,
               process_status, created_by, created_date, updated_date,
               updated_by, currency_code, check_number,
               transaction_amount, bank_to_bank_info, foreign_transaction,
               sender_aba_routing_number, receiver_aba_routing_number, beneficiary_name, 
               originator_name, ach_addenda, loan_id, loan_type_id]

        writer.writerow(row)


file.close()
