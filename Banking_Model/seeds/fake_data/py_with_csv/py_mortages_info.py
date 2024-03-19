import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'mortages_info.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['property_id','property_address','property_type','property_value',
              'year_built','property_square_footage','property_owner_name','property_owner_phonenumber' 
              , 'property_owner_email_id', 'property_tax_rate']
    writer.writerow(header)
   
    for i in range(1, 100001):
        property_id = i
        property_address = fake.street_address()
        property_type =  random.choice(['Single-Family','Condominium ','Townhouse',
                                        'Apartment','Commercial ','Vacant Land','Mixed-Use Property'])
        property_value = random.randint(10020000,1113000000)
        year_built = fake.date_between(start_date='-30y', end_date='-1y').year
        property_square_footage = random.randint(500, 5000)
        property_owner_name = fake.name()
        property_owner_phonenumber = random.randint(100000000, 9999999999)
        property_owner_email_id = fake.email()
        property_tax_rate = fake.random.uniform(0.5, 2.5)
        
        row = [property_id, property_address, property_type , property_value , year_built ,
                property_square_footage , property_owner_name , property_owner_phonenumber , 
                property_owner_email_id , property_tax_rate]
        
        writer.writerow(row)


file.close()            