import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_securities.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['security_id','security_name','security_type','quantity','market_value','purchase_date',
             'purchase_price','purchase_value']
    writer.writerow(header)
    for i in range(1, 5000):
        security_type = random.choice(['Government Bonds','Corporate Bonds','Treasury Bills','Mortgage-Backed Securities','Asset-Backed Securities','Equity Securities',
                                    'Money Market Instruments','Foreign Exchange'])
        security_name = fake.company().replace(',', '')+' '+ 'securities'
        quantity =  random.randint(1,100)
        market_value = random.randint(5000,100000)
        purchase_date = fake.date_between(start_date='-7y', end_date='today')
        random_value = random.randint(100,700)
        purchase_price = int((market_value-random_value) / quantity)
        purchase_value = market_value-random_value
       
          
        row = [i,security_name,security_type,quantity,market_value,purchase_date,purchase_price,purchase_value]
        
        writer.writerow(row)


file.close()