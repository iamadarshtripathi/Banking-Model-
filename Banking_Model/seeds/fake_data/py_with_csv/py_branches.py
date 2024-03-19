import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'branches.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['branch_id','branch_name','branch_code', 'branch_head_id','branch_address_line1',
              'branch_address_line2','branch_city','branch_state','branch_zip','branch_telephone',
              'email_address']
    writer.writerow(header)
    for i in range(1, 30000):
        branch_name = fake.city()
        branch_code = random.randint(10000,99999)
        branch_head_id = random.randint(1,20000)
        branch_address_line1 = fake.street_name()
        branch_address_line2 = fake.street_address()
        branch_city = fake.city()
        branch_state = fake.state()
        branch_zip = fake.postalcode()
        branch_telephone = fake.phone_number()
        email_address = fake.email()
        


        row = [i , branch_name, branch_code, branch_head_id , branch_address_line1 ,branch_address_line2 ,
                    branch_city, branch_state, branch_zip, branch_telephone, email_address]
        
        writer.writerow(row)


file.close()