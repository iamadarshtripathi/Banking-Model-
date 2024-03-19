import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'loan_applications.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['application','property_id','loan_type_id','loan_amount',
              'customer_id','account_id','applied_date','employee_id']
    writer.writerow(header)
   
    for i in range(1,50001):
        property_id = random.randint(1,100000)
        loan_type_id = random.randint(1,40)
        loan_amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
        customer_id = random.randint(1,100001)
        account_id = random.randint(1,33344)
        applied_date = fake.date_between(start_date = '-7300d', end_date = 'today')
        employee_id = random.randint(1,40001)
        
        row = [i,property_id, loan_type_id, loan_amount, customer_id,
                account_id, applied_date, employee_id]
        
        writer.writerow(row)


file.close()                