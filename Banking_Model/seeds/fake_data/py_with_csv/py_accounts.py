import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'accounts.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['account_id','customer_id','account_type_id','branch_id',
              'account_open_date','account_close_date','account_status','created_by','updated_by',
              'created_date','updated_date','account_number','account_application_id']
    writer.writerow(header)
    for i in range(1,33343):
    
        customer_id = random.randint(1, 100000)
        account_type_id = random.randint(1,18)
        branch_id = random.randint(1, 30000)    
        account_number = fake.random_number(digits=10)
        account_status = random.choice(['Open','Close']) #change 
        account_open_date = fake.date_between(start_date = '-2000d', end_date = 'today')
        if(account_status=='Open'):
            account_close_date = None
        else:
            account_close_date = fake.date_between(start_date = account_open_date, end_date = 'today') 
        created_by = fake.name()
        updated_by = fake.name()
        created_date = fake.date_between(start_date = '-2000d', end_date = 'today')
        updated_date = fake.date_between(start_date = '-1000d', end_date = 'today')
        account_application_id = None
       
        
        row = [i, customer_id,account_type_id, branch_id, account_open_date, account_close_date,
                    account_status, created_by, 
                    updated_by, created_date,
                    updated_date, account_number, account_application_id]
        
        writer.writerow(row)


file.close()