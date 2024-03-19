

import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'fixed_deposit.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['fd_id', 'customer_id', 'certificate_number', 'amount', 'from_date'
              ,'to_date', 'period','interest_rate', 'automatic_renewal', 
                'issued_date,payment_method','receipt_number']
    writer.writerow(header)
   
    period_options = {
    1: 'days',
    2: 'weeks',
    3: 'months',
    4: 'years'
    }

    for i in range(1, 30000):
        customer_id = random.randint(1,100001)
        certificate_number = fake.aba()
        amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
        from_date = fake.date_between(start_date = '-1100d', end_date = 'today')
        to_date = fake.date_between(start_date = 'today', end_date = '+1100d')
        period = (to_date - from_date).days
        interest_rate = fake.pyfloat(positive=True, min_value=1.0, max_value=3.0)
        automatic_renewal = fake.boolean()
        issued_date = from_date 
        payment_method = random.choice(['Credit Card', 'Debit Card', 'Net Banking', 'PayPal'])
        receipt_number = fake.aba()
        
        row = [i, customer_id, certificate_number, amount, from_date, to_date, period,
                    interest_rate, automatic_renewal, 
                    issued_date,payment_method,
                    receipt_number]
        
        writer.writerow(row)


file.close()    