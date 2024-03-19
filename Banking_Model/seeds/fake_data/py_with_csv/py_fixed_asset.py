import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_fixed_asset.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['asset_id','asset_type','purchase_date','purchase_cost','current_cost']
    writer.writerow(header)
    for i in range(1, 5000):
        asset_type = random.choice(['Building','Land','Equipment','Vehicle','IT Software'])
        purchase_date = fake.date_between(start_date='-10y', end_date='today')
        expense_amount = random.randint(1000,10000)
        department_id = random.randint(1,9)
        if(asset_type == 'Building'):
            purchase_cost = random.randint(100000,1000000)
            current_cost = random.randint(10000,100000) + purchase_cost
        if(asset_type == 'Land'):
            purchase_cost = random.randint(100000,500000)
            current_cost = random.randint(100000,500000) + purchase_cost
        if(asset_type == 'Equipment'):
            purchase_cost = random.randint(5000,10000)
            current_cost = random.randint(1000,5000)
        if(asset_type == 'Vehicle'):
            purchase_cost =random.randint(10000,50000)
            current_cost = random.randint(5000,10000)
        if(asset_type == 'IT Software'):
            purchase_cost =random.randint(1000,20000)
            current_cost = random.randint(0,1000) + purchase_cost
          
        row = [i,asset_type,purchase_date,purchase_cost,current_cost]
        
        writer.writerow(row)


file.close()