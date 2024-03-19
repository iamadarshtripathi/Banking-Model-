

import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'checkbook.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['account_id','requested_date','date_issued','ready_date',
              'delivery_date','check_number_from','check_number_to']
    writer.writerow(header)
   
    for i in range(1,20000 ):
        requested_date = fake.date_between(start_date = '-500d', end_date = '-20d')
        date_issued = requested_date + timedelta(days=10) #fake.date_between(start_date = '-190d', end_date = '-10d')
        account_id = random.randint(1,33342)
        check_number_from = random.randint(100200,111300)
        check_number_to = check_number_from + random.choice([10, 20, 30, 50])
        #ready_date = fake.date_between(start_date = '-180d', end_date = '-5d')
        ready_date = date_issued + timedelta(days=2)
        delivery_date = ready_date + timedelta(days=5)
        #delivery_date = date_issued + fake.date_between(start_date = '-1800d', end_date = '-1750d')
        
        row = [account_id, requested_date, date_issued, ready_date, delivery_date, check_number_from, check_number_to]
        
        writer.writerow(row)


file.close()    