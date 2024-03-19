import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_non_performing_loan.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['non_performing_loan_id','loan_status','last_payment_date','description',
              'loan_id']
    writer.writerow(header)
   
    for i in range(1,80):
        loan_status = random.choice(['open','closed'])
        last_payment_date = fake.date_between(start_date='-1000d', end_date='today')
        description = random.choice(['late repayment','full repayment'])
        loan_id = random.randint(1,1000)
        
        row = [i,loan_status, last_payment_date, description, loan_id]
        
        writer.writerow(row)


file.close()                