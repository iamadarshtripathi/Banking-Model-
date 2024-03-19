

import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'employees.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['employee_id' , 'first_name', 'last_name' , 'email_address' ,'employee_address_line1','employee_address_line2' , 
              'residence_number' ,'employee_city', 'employee_state', 'employee_zip' ,  'phone_number' , 'grade' ,
              'tiered_level' ,'branch_id']
    writer.writerow(header)
   
    for i in range(1, 40000):
       
        first_name = fake.first_name()
        last_name = fake.last_name()
        email_address = fake.email()
        employee_address_line1 = fake.street_name()
        employee_address_line2 = fake.street_address()
        residence_number = random.randint(10000000,99999999)        
        employee_city = fake.city()    
        employee_state = fake.state()
        employee_zip = fake.postalcode()    
        phone_number = fake.phone_number()
        grade = fake.job()
        grade = grade.split(',')[0]
        grade = grade[0:48]
        tiered_level = random.choice(['T1','T2','T3','T4'])
        branch_id = random.randint(1,30000)
 
        row = [i, first_name , last_name , email_address , employee_address_line1 , employee_address_line2 , residence_number ,
               employee_city , employee_state , employee_zip , phone_number , grade , tiered_level , branch_id]
        
        writer.writerow(row)


file.close()    