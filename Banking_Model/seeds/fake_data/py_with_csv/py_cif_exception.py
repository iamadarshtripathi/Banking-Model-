import csv
from faker import Faker
import random
from datetime import date, timedelta


fake = Faker()
num_rows = 10000
filename = 'csv_cif_exception.csv'

with open(filename, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['exception_id', 'customer_id', 'employee_id', 'open_date', 'review_date',
                     'doc_status','description','comments'])
    
    for i in range(1, num_rows+1):
        exception_id = i
        customer_id = random.randint(1,29000)
        employee_id = random.randint(1,29000)
        open_date = fake.date_between(start_date='-4y', end_date='today')

        current_date = date.today()
        difference = (current_date - open_date).days
        
        if difference <= 150:
            review_date = open_date
        else:
            review_date =  open_date + timedelta(days=random.randint(50,130))

        
        doc_status = random.choice(['Missing','Exception'])
        description = random.choice(['Identity','CIP Exception'])
        if(description == 'CIP Exception'):
            comments = random.choice(['Please re-scan ID Verification','ID has Expired'])
        else:
            comments = None 
          

        writer.writerow([exception_id, customer_id, employee_id , open_date , review_date , doc_status ,
                         description , comments])
    csvfile.close()