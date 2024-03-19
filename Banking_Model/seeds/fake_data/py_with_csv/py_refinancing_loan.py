import csv
from faker import Faker
import random

fake = Faker()

csv_file_path = 'refinancing_loan.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['refinancing_id', 'loan_id' ,'refinancing_type', 'refinancing_loan_amount','originating_date','closing_date','appraised_value']
    
    for i in range(1, 1001):
        refinancing_id = i
        loan_id = random.randint(1,4000)
        if loan_id%20 == 0 and loan_id%2 == 0:
            loan_id = loan_id + 4005
        elif loan_id%20 == 0 and loan_id%2 == 1:
            loan_id = loan_id + 4006
        refinancing_type = random.choice(['Cashout','Fixed-rate'])
        refinancing_loan_amount =  random.randint(100200,1113000)
        originating_date = fake.date_between(start_date='-30y', end_date='-1y').year
        closing_date = originating_date + 15
        appraised_value = refinancing_loan_amount + 1000000


        row = [refinancing_id, loan_id ,refinancing_type, refinancing_loan_amount,originating_date,closing_date,appraised_value]
        
        writer.writerow(row)


file.close()    