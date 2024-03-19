import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'customers.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['customer_id','customer_name','customer_address','customer_address_line2',
              'customer_city','customer_state','customer_zip','customer_phone','customer_type',
              'customer_taxpayer_id' , 'cif_number' ,'created_by','updated_by','created_date','updated_date','customer_ssn','customer_email',
              'monthly_income','net_asset_values','net_liabilities']
    writer.writerow(header)
    for i in range(1, 100001):
        customer_name = fake.name()
        customer_address = fake.street_name()
        customer_address_line2 = fake.street_address()
        customer_city = fake.city()
        customer_state = fake.state()
        customer_zip = fake.postalcode()
        customer_phone = fake.phone_number()
        customer_type = random.choice(['Individual','Corporate','Joint'])
        customer_taxpayer_id = random.randint(10000,1000000)
        cif_number = 'CIF'+str(i)
        created_by = fake.name()
        updated_by = fake.name()
        customer_ssn = fake.ssn()
        customer_email = fake.email()
        created_date = fake.date_time_between(start_date = '-2000d', end_date = 'now')
        updated_date = fake.date_time_between(start_date = '-100d', end_date = 'now')
        monthly_income =  random.randint(1000, 100000)  
        net_asset_values = random.randint(10000, 10000000) 
        net_liabilities = random.randint(100, 100000) 

        row = [i,customer_name,customer_address,customer_address_line2,
              customer_city,customer_state,customer_zip,customer_phone,customer_type,
              customer_taxpayer_id,cif_number,created_by,updated_by,created_date,updated_date, customer_ssn,customer_email
              ,monthly_income,net_asset_values,net_liabilities]
        
        writer.writerow(row)


file.close()