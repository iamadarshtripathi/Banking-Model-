import csv
from faker import Faker
import random

fake = Faker()

# Generate data for each row
def generate_row(i):
    transfer_id = i+1
    transfer_type = random.choice(['IN', 'OUT'])
    loan_amount = random.randint(100000, 500000)
    
    if transfer_type == 'IN':
       
        other_interest_rate = round(random.uniform(4, 15), 2)
        bank_interest_rate = round(random.uniform(4, other_interest_rate), 2)
    else:
        other_interest_rate = round(random.uniform(4, 15), 2)
        bank_interest_rate = round(random.uniform(other_interest_rate, 15), 2)
        
    return [transfer_id, transfer_type, loan_amount, bank_interest_rate, other_interest_rate]

# Generate data for multiple rows
def generate_data(num_rows):
    data = [['Transaction_ID', 'Transfer_Type', 'Loan_Amount', 'Bank_Interest_Rate', 'Other_Interest_Rate']]
    
    for i in range(num_rows):
        data.append(generate_row(i))
    
    return data

# Write data to CSV file
def write_to_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)
    print(f"Data saved to {filename}")

# Generate and write data to CSV file
num_rows = 1000  # Adjust the number of rows as needed
data = generate_data(num_rows)
filename = 'transfer_loan_data.csv'
write_to_csv(data, filename)
