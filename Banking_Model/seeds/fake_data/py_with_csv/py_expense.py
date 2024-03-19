import csv
from faker import Faker
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

fake = Faker()

csv_file_path = 'csv_expense.csv'

# Write data to the CSV file
with open(csv_file_path, 'w', newline='') as file:

    writer = csv.writer(file)

    header = ['expense_id','expense_date','expense_amount','description',
              'department_id']
    writer.writerow(header)
    for i in range(1, 10000):
        expense_date = fake.date_between(start_date='-4y', end_date='today')
        expense_amount = random.randint(1000,10000)
        department_id = random.randint(1,9)
        if(department_id == 1):
            description = random.choice(['expense related to savings account','expense related to checking account','expense related to credit cards'])
        if(department_id == 2):
            description = random.choice(['expense related to cash management for businesses','expense related to commercial loans','expense related to trade finance'])
        if(department_id == 3):
            description = random.choice(['expense related to capital market operations','expense related to mergers and acquisitions'])
        if(department_id == 4):
            description = random.choice(['expense related to investment management','expense related to financial planning','expense related to personalized advisory services for affluent customers'])
        if(department_id == 5):
            description = random.choice(['expense related to back-office operations','expense related to technology upgrades','expense related to cybersecurity measures'])
        if(department_id == 6):
            description = random.choice(['expense related to risk assessment','expense related to internal controls','expense related to compliance monitoring'])
        if(department_id == 7):
            description = random.choice(['expense related to treasury activities','expense related to liquidity management','expense related to trading operations'])
        if(department_id == 8):
            description = random.choice(['expense related to employee recruitment','expense related to training and development','expense related to performance management'])
        if(department_id == 9):
            description = random.choice(['expense related to marketing campaigns','expense related to customer acquisition strategies','expense related to market research'])            

          
        row = [i,expense_date,expense_amount,description,
              department_id]
        
        writer.writerow(row)


file.close()