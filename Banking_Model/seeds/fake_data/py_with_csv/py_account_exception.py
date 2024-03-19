import csv
from faker import Faker
import random
from datetime import *


fake = Faker()
num_rows = 6000
filename = 'csv_account_exception.csv'

with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['account_exception_id', 'account_id', 'employee_id', 'exception_date', 'exception_status',
                     'exception_type','description'])
    
    for i in range(1, num_rows+1):
        account_exception_id = i
        account_id = random.randint(1,29000)
        employee_id = random.randint(1,29000)
        exception_date = fake.date_between(start_date='-4y', end_date='today')
        exception_status = random.choice(['In progress','Resolved','Under investigation'])
        exception_type = random.choice(['Overdrawn account','Unauthorized transaction', 'Account freeze'])
        if(exception_type == 'Overdrawn account'):
            description = 'Account balance dropped below zero'
        elif(exception_type == 'Unauthorized transaction'):    
            description = 'Suspicious transaction detected on the account'
        else:
            description = 'Account temporarily frozen due to suspected fraud'    
          

        writer.writerow([account_exception_id, account_id, employee_id , exception_date , exception_status ,
                         exception_type , description])
    csvfile.close()