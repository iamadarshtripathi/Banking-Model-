

import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'customer_satisfaction.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['satisfaction_id', 'application_id', 'loan_id','satisfaction_score','feedback,date']
    writer.writerow(header)
   
    for i in range(1, 1001):
        satisfaction_id = i
        application_id = random.randint(1,10000)
        loan_id = random.randint(1,10000)
        satisfaction_score = random.randint(1,5)
        feedback = fake.paragraph()
        feedback = feedback[0:220]
        date = fake.date_between(start_date='-2y', end_date='-1y')
        row = [satisfaction_id, application_id, loan_id,satisfaction_score,feedback,date]
        writer.writerow(row)


file.close()    