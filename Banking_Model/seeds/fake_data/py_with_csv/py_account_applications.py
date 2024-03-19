import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'account_applications.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['account_application_id','application_status','applied_date','application_type_id',
              'account_application_state']
    writer.writerow(header)
    for i in range(1,100000):
        application_status = random.choice(['Completed','Incomplete','Denied'])
        applied_date = fake.date_between(start_date = '-1095d', end_date = 'today')
        application_type_id = random.randint(1, 3)
        account_application_state = fake.state()

 

        row = [i,application_status,applied_date, application_type_id, account_application_state]
        
        writer.writerow(row)


file.close()