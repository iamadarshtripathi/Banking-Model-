import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'card_details.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['card_id','account_id','customer_id','card_type','card_number','card_limit','issued_date','valid_date','card_sub_type']
    writer.writerow(header)
     
    for i in range(1,40000):
        
        card_type = random.choice(['DEBIT','CREDIT'])
        if(card_type=='DEBIT'):
            account_id = round(random.uniform(1,33343))
            customer_id = None
        else:
            account_id = None
            customer_id = round(random.uniform(1,100001))
        card_number = fake.random_number(digits=16)
        if(card_type=='DEBIT'):
            card_limit = None
        else:
            card_limit = random.randint(2000,10000)
        issued_date = fake.date_time_between(start_date = '-1095d', end_date = 'now')
        if(card_type=='DEBIT'):
            valid_date = issued_date + relativedelta(years=4)
        else:
            valid_date = issued_date + relativedelta(years=3)
        if(card_type=='DEBIT'):
            card_sub_type = random.choice(['UnionPay','American Express','Discover','MasterCard','Visa'])
        else:
            card_sub_type = random.choice(['Travel','Business','Reward','Shopping'])

        row = [i,account_id,customer_id,card_type,card_number,card_limit,issued_date,valid_date,card_sub_type]
        writer.writerow(row)


file.close()
