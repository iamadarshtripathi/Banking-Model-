import csv
from faker import Faker
import random

fake = Faker()

csv_file_path = 'csv_uninsured_deposits.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['non_performing_account_id','non_performing_amount','tier1','risk_weighted_assets','created_date','account_id','description']
    writer.writerow(header)

    for i in range(1, 40000):

        non_performing_account_id = fake.random_int(min=1, max=40000)
        non_performing_amount = fake.random_int(min=100000, max=999999)
        tier1 = fake.random_int(min=100000, max=999999)
        risk_weighted_assets = fake.random_int(min=100000, max=999999)
        created_date = fake.date_between(start_date='-4y', end_date='today')
        account_id = random.randint(1, 40000)
        description = random.choice(['abc','xyz'])
        
        row = [non_performing_account_id,non_performing_amount,tier1,risk_weighted_assets,created_date, account_id, description]
        
        writer.writerow(row)

file.close()