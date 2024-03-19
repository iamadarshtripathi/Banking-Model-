import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_capital_expenditure.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['expenditure_id','asset_id','expenditure_date','expenditure_amount']
    writer.writerow(header)
    for i in range(1, 3000):
        asset_id = random.randint(1,4500)
        expenditure_amount = random.randint(100,750)
        expenditure_date = fake.date_between(start_date='-4y', end_date='today')
       
          
        row = [i,asset_id,expenditure_date,expenditure_amount]
        
        writer.writerow(row)


file.close()