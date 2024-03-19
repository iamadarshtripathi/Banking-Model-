import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'customer_exception.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['exception_id', 'customer_id', 'employee_id','Open_date','Review_date,doc_status, description, comments']
    writer.writerow(header)
   
    for i in range (1,1001):
        customer_id = random.randint(1,29000)
        employee_id = random.randint(1,29000)
        open_date = fake.date_between(start_date='-5000d', end_date='-1000d')
        review_date =  open_date + timedelta(days=random.randint(500,600))
        doc_status = random.choice(['Missing','Exception'])
        description = random.choice(['Identity','CIP Exception'])
        if(description == 'CIP Exception'):
            comments = random.choice(['Please re-sacn ID Verification','ID has Expired'])
        else:
            comments = None 
        row = [i, customer_id, employee_id, open_date, review_date , doc_status , description , comments]
        writer.writerow(row)


file.close()    