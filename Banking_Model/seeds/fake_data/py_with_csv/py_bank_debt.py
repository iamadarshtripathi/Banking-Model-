import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_bank_debt.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['debt_id','debtor_name','debt_type','principal_amount','interest_rate','issue_date','maturity_date',
             'collateral','guarantee','debt_status']
    writer.writerow(header)
    for i in range(1, 70):
        debt_type = random.choice(['Bonds','loan'])
        debtor_name = fake.name
        principal_amount = random.randint(1000000,10000000)
        interest_rate = random.randint(1,5)
        issue_date = fake.date_between(start_date='-4y', end_date='today')
        maturity_date = issue_date + timedelta(days=365*5)
        collateral = random.choice(['property','none'])
        guarantee = fake.company().replace(',', '') if collateral == 'none' else 'none'
        debt_status = 'inactive' if maturity_date < datetime.now().date() else 'active'
       
          
        row = [i,debtor_name,debt_type,principal_amount,interest_rate,issue_date,maturity_date,collateral,guarantee,debt_status]
        
        writer.writerow(row)


file.close()