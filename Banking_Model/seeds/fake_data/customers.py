import snowflake.connector
import configparser
from faker import Faker
import random
import string
from datetime import *
from faker.providers import credit_card
import textwrap
from dateutil.relativedelta import relativedelta

# Initialize Faker
fake = Faker()

#Initialize Config File and Read
config = configparser.ConfigParser()
config.read('snowflake.env')

# Initialize Snowflake connection
conn = snowflake.connector.connect(
    user=config.get('Snowflake','user'),
    authenticator=config.get('Snowflake','authenticator'),
    account=config.get('Snowflake','account'),
    warehouse=config.get('Snowflake','warehouse'),
    database=config.get('Snowflake','database'),
    schema=config.get('Snowflake','schema'),
)
# Initialize Snowflake cursor
cur = conn.cursor()

#Dictonary for Account Types and Description
account_dict = {
  "CA": "Checking Accounts",
  "SA": "Savings accounts",
  "MMA": "Money Market of accounts",
  "CD" : "Certificates of deposit",
  "BA" : "Brokerage account",
  "IRA" : "Individual retirement accounts "
}


# Account Type Table
# i = 1
# for key in account_dict.keys():
#     account_type = key 
#     account_type_description = account_dict[key]

#     cur.execute("INSERT INTO ACCOUNT_TYPES VALUES (%s, %s, %s)",
#     (i, account_type,account_type_description))
#     i = i+1

#Dictonary for transaction_formats and Description
transaction_formats_dict = {
  "CT": "Cash Transaction",
  "CT": "Credit Transaction",
  "ST": "Sales Transaction",
  "PT" : "Purchase Transaction",
  "JE" : "Journal Entries",
  "ET" : "Electronic Transaction",
  "NMT" : "Non-Monetary Transaction"
}


# Transaction Formats Table
#Generate and insert into Transaction Formats
# i = 1
# for key in transaction_formats_dict.keys():
#     format_type = key #random.choice(list(account_dict.keys()))
#     format_type_description = transaction_formats_dict[key]

#     cur.execute("INSERT INTO TRANSACTION_FORMATS VALUES (%s, %s, %s)",
#     (i, format_type,format_type_description))
#     i = i+1

# #Dictonary for Transaction Types and Description
transaction_type_dict = {
  "SA": "Sales",
  "PU": "Purchases",
  "PA": "Payments",
  "RE" : "Receipts",
  "EX" : "Expenses",
  "REV" : "Revenues",
  "IN" : "Investments",
  "LO" : "Loans",
  "DE" : "Depreciation",
  "DI" : "Dividends",
  "ACH" : "ACH",
  "CHK": "Check",
  "WI": "Wire",
  "TR": "Transfer"
}

# cur.execute("USE WAREHOUSE SCT_SGARG_WH")

# Transaction Type Table
#Generate and insert Transaction Type data
# # i = 1
# # for key in transaction_type_dict.keys():
# #     transaction_type = key 
# #     transaction_type_description = transaction_type_dict[key]

# #     cur.execute("INSERT INTO TRANSACTION_TYPES VALUES (%s, %s, %s)",
# #     (i, transaction_type,transaction_type_description))
# #     i = i+1

#DO NOT UNCOMMENT THIS CODE - WE ARE USING CSV FILE TO LOAD LOAD TYPE DATA
#Dictonary for Loan Types and Description
# loan_type_dict = {
#     "1":["CAR","2.5","24"],
#     "2":["HOME","2","60"],
#     "3":["BUSINESS","4","15"], 
#     "4":["PERSONAL","3.24","12"],
#     "5":["EDUCATION","1.2","96"]
# }


# for key in loan_type_dict.keys():
#     loan_type_id = key 
#     values=loan_type_dict[key]
#     loan_type = values[0]
#     interest = float(values[1])
#     no_of_emi =int(values[2])

#     cur.execute("INSERT INTO LOAN_TYPES VALUES (%s, %s, %s,%s)",
#     (loan_type_id,loan_type,interest,no_of_emi))
    

# Generate and insert random customer data
# # for i in range(1, 101):
# #     customer_name = fake.name()
# #     customer_address = fake.street_name()
# #     customer_address_line2 = fake.street_address()
# #     customer_city = fake.city()
# #     customer_state = fake.state()
# #     customer_zip = fake.postalcode()
# #     customer_phone = fake.phone_number()
# #     customer_type = random.choice(['Individual','Corporate','Joint'])
# #     customer_taxpayer_id = random.randint(10000,1000000)
# #     created_by = fake.name()
# #     updated_by = fake.name()
# #     customer_ssn = fake.ssn()
# #     customer_email = fake.email()
# #     created_date = fake.date_time_between(start_date = '-2000d', end_date = 'now')
# #     updated_date = fake.date_time_between(start_date = '-100d', end_date = 'now')
# #     monthly_income =  random.randint(1000, 100000)  
# #     net_asset_values = random.randint(10000, 10000000) 
# #     net_liabilities = random.randint(100, 100000) 

    
# #     cur.execute("INSERT INTO customers VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
# #                 (i, customer_name, customer_address, customer_address_line2, customer_city, customer_state,
# #                  customer_zip, customer_phone, 
# #                  customer_type, i,
# #                  created_by, updated_by, created_date, updated_date, customer_ssn, customer_email , monthly_income , net_asset_values , net_liabilities))

# Account Application Table
# for i in range(1,40000):
#     application_status = random.choice(['Completed','Incomplete','Denied'])
#     applied_date = fake.date_between(start_date = '-1095d', end_date = 'today')
#     application_type_id = random.randint(1, 3)
#     account_application_state = fake.state()

#     cur.execute("INSERT INTO account_application VALUES (%s, %s, %s, %s, %s)", 
#                 (i,application_status,applied_date, application_type_id, account_application_state))
 

#Accounts Table
# for i in range(1,201):
    
#     customer_id = random.randint(1, 100)
#     account_type_id = random.randint(1,6)
#     branch_id = random.randint(1, 100)    
#     account_number = fake.random_number(digits=10)
#     account_status = random.choice(['Open','Close']) #change 
#     account_open_date = fake.date_between(start_date = '-2000d', end_date = 'today')
#     if(account_status=='Open'):
#        account_close_date = None
#     else:
#         account_close_date = fake.date_between(start_date = account_open_date, end_date = 'today') 
#     created_by = fake.name()
#     updated_by = fake.name()
#     created_date = fake.date_between(start_date = '-2000d', end_date = 'today')
#     updated_date = fake.date_between(start_date = '-100d', end_date = 'today')
    # account_application_id = random.randint(1, 100)
    
    
#     cur.execute("INSERT INTO accounts VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
#                 (i, customer_id,account_type_id, branch_id, account_open_date, account_close_date,
#                  account_status, created_by, 
#                  updated_by, created_date,
#                  updated_date, account_number))




# Generate and insert random Transactions data
# loan_id_value=1
# for i in range(1, 1001):
#     account_id = random.randint(1,200)    
#     transaction_type_id = random.randint(1,14)
#     if (transaction_type_id == 8) :
#         format_id = 1
#     else :
#         format_id = random.randint(2,7)    
#     source = random.choice(['SWIFT MT','ACH','FIRD', 'BAI'])
#     filename = fake.file_name() +"_" + fake.date()
#     sender =fake.first_name()
#     receiver = fake.first_name()
#     transaction_date = fake.date_between(start_date = '-100d', end_date = 'today')
#     process_date = fake.date_between(start_date = '-100d', end_date = 'today')
#     process_status = random.choice(['Successful','Failed'])
#     created_by = fake.name()
#     created_date = fake.date_between(start_date = '-2000d', end_date = 'today')
#     updated_date = fake.date_between(start_date = '-100d', end_date = 'today')
#     updated_by = fake.name()
#     currency_code = fake.currency_code()
#     check_number = fake.aba()
#     transaction_amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
#     bank_to_bank_info = fake.file_name()
#     foreign_transaction = fake.boolean()
#     sender_aba_routing_number = fake.aba()
#     receiver_aba_routing_number = fake.aba()
#     beneficiary_name = fake.name()
#     originator_name = fake.name()
#     ach_addenda = sender+'_'+process_status+'_'+receiver
#     if(transaction_type_id==8 ):
#         loan_id = loan_id_value
#         loan_id_value = loan_id_value + 1
#         employee_id=random.randint(1,100)
#     else:
#          loan_id = None
#     if(transaction_type_id==8):
#         loan_type_id = random.randint(1,5)
#     else:
#         loan_type_id = None
#     cur.execute("INSERT INTO transactions VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s,%s, %s,%s, %s, %s, %s,%s,%s)", 
#                 (i, account_id, format_id, transaction_type_id, source, filename,
#                  sender, receiver, transaction_date, process_date,
#                  process_status, created_by, created_date, updated_date,
#                  updated_by, currency_code,check_number ,
#                  transaction_amount, bank_to_bank_info, foreign_transaction,
#                  sender_aba_routing_number, receiver_aba_routing_number, beneficiary_name
#                  , originator_name, ach_addenda,loan_id,loan_type_id))



# Generate and insert random transfer_transaction data
# for i in range(1, 11):
#     transaction_id  = random.randint(1,11)
#     beneficiary_name = fake.name()
#     originator_name = fake.name()


#     cur.execute("INSERT INTO TRANSFER_TRANSACTIONS VALUES (%s, %s, %s , %s)", 
#                 (i, transaction_id, beneficiary_name, originator_name))
    


# Generate and insert random Transaction item data
# for i in range(1, 11):
#     transaction_id = random.randint(1,11)
#     group = random.randint(1,11)
#     line_item = random.randint(1,11)
#     sequence = fake.

#     cur.execute("INSERT INTO transactions_items VALUES (%s, %s, %s, %s, %s)", 
#                 (i, transaction_id, group, line_item, sequence))


# Generate and insert random ACH Transaction data
# for i in range(1, 11):
#     transaction_id = random.randint(1,11)
#     ach_currency_code = random.randint(1,11)
#     ach_addenda_char = random.randint(1,11)
#     credit_amount = 
#     debit_amount =

#     cur.execute("INSERT INTO ach_transactions VALUES (%s, %s, %s, %s, %s,%s)", 
#                 (i, transaction_id, ach_currency_code, ach_addenda_char, credit_amount, debit_amount))


# Generate and insert random Check transaction data
# for i in range(1, 11):
#     transaction_id = random.randint(1,11)
#     check_number = random.randint(1,11)
#     credit_amount = random.randint(1,11)
#     debit_amount = 

#     cur.execute("INSERT INTO check_transactions VALUES (%s, %s, %s, %s, %s,%s)", 
#                 (i, transaction_id, check_number, credit_amount, debit_amount))

# Generate and insert random Loan Details data
# for i in range (1,101):
#     account_number = random.randint(1,200)
#     loan_type = random.choice(['CAR','HOME','BUSINESS', 'PERSONAL','EDUCATION'])
#     loan_amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
#     sanction_date = fake.date()
#     emi_amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0,max_value=loan_amount)
#     emi_date = fake.date()
#     current_interest_rate = fake.pyfloat(positive=True, min_value=1.0, max_value=3.0)
#     interest_rate_perc = fake.pyfloat(positive=True, min_value=1.0, max_value=3.0)
#     cur.execute("INSERT INTO loan_details VALUES (%s, %s, %s, %s, %s,%s,%s,%s)", 
#                  (i, account_number, loan_type, loan_amount, sanction_date,
#                   emi_amount, emi_date, current_interest_rate))







# Generate and insert random atm card data
# fake.add_provider(credit_card)
# for i in range(1, 101):
#     card_number = fake.credit_card_number()
#     issued_date = fake.date_between(start_date = '-2000d', end_date = 'today')

#     cur.execute("INSERT INTO atm_card VALUES (%s, %s, %s)", 
#                 (card_number, i, issued_date))
    



#  Generate and insert random branch data
# for i in range(1, 101):
#     branch_name = fake.city()
#     branch_code = random.randint(10000,99999)
#     branch_head_id = random.randint(100000,999999)
#     branch_address_line1 = fake.street_name()
#     branch_address_line2 = fake.street_address()
#     branch_city = fake.city()
#     branch_state = fake.state()
#     branch_zip = fake.postalcode()
#     branch_telephone = fake.phone_number()
#     email_address = fake.email()
    
    
#     cur.execute("INSERT INTO branches VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
#                 (i , branch_name, branch_code , branch_head_id, branch_address_line1, branch_address_line2,
#                  branch_city, branch_state, branch_zip, branch_telephone, email_address))
    




# Generate and insert random employees data
# for i in range(1, 101):
#     tiered_level = random.choice(['T1','T2','T3','T4'])
#     branch_id = random.randint(1,101)
#     first_name = fake.first_name()
#     last_name = fake.last_name()
#     grade = fake.job()
#     employee_address_line1 = fake.street_name()
#     employee_address_line2 = fake.street_address()
#     employee_city = fake.city()    
#     employee_state = fake.state()
#     employee_zip = fake.postalcode()    
#     residence_number = random.randint(10000000,99999999)
#     email_address = fake.email()
      # phone_number = fake.phone_number()
    
    
#     cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s)", 
#                 (i , first_name, last_name, email_address, employee_address_line1 , employee_address_line2 , 
#                     residence_number , employee_city, employee_state, employee_zip , phone_number , grade , tiered_level , branch_id))
    



# Generate and insert random fixed deposit data
# period_options = {
#     1: 'days',
#     2: 'weeks',
#     3: 'months',
#     4: 'years'
# }
# for i in range(1, 101):
#     customer_id = random.randint(1,100)
#     certificate_number = fake.aba()
#     amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
#     from_date = fake.date_between(start_date = '-1100d', end_date = 'today')
#     to_date = fake.date_between(start_date = 'today', end_date = '+1100d')

# #     #generating random period of fixed deposit
#     # period_unit = period_options[fake.pyint(min_value=1, max_value=4)]
#     # # Generate a random number of periods based on the unit
#     # if period_unit == 'days':
#     #     periods = fake.pyint(min_value=7, max_value=365)
#     # elif period_unit == 'weeks':
#     #     periods = fake.pyint(min_value=1, max_value=52)
#     # elif period_unit == 'months':
#     #     periods = fake.pyint(min_value=1, max_value=120)
#     # else:
#     #     periods = fake.pyint(min_value=1, max_value=10)
#     # period = f"{periods} {period_unit}"
#     period = (to_date - from_date).days
#     interest_rate = fake.pyfloat(positive=True, min_value=1.0, max_value=3.0)
#     automatic_renewal = fake.boolean()
#     issued_date = from_date #fake.date_between(start_date = '-100d', end_date = 'today')
#     payment_method = random.choice(['Credit Card', 'Debit Card', 'Net Banking', 'PayPal'])
#     receipt_number = fake.aba()
    
#     cur.execute("INSERT INTO fixed_deposit VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
#                 (i, customer_id, certificate_number, amount, from_date, to_date, period,
#                  interest_rate, automatic_renewal, 
#                  issued_date,payment_method,
#                  receipt_number))
    





# Generate and insert random check book data
# for i in range(1, 101):
#     requested_date = fake.date_between(start_date = '-200d', end_date = '-20d')
#     date_issued = requested_date + timedelta(days=10) #fake.date_between(start_date = '-190d', end_date = '-10d')
#     account_id = random.randint(1,200)
#     check_number_from = random.randint(100200,111300)
#     check_number_to = check_number_from + random.choice([10, 20, 30, 50])
#     #ready_date = fake.date_between(start_date = '-180d', end_date = '-5d')
#     ready_date = date_issued + timedelta(days=2)
#     delivery_date = ready_date + timedelta(days=5)
#     #delivery_date = date_issued + fake.date_between(start_date = '-1800d', end_date = '-1750d')
    
    
#     cur.execute("INSERT INTO check_book VALUES (%s, %s, %s, %s, %s, %s, %s)", 
#                 (account_id, requested_date, date_issued, ready_date, delivery_date, check_number_from, check_number_to))


# Generate and insert random Loan Applications data
# for i in range(1,100):
#     property_id = random.randint(1,200)
#     loan_type_id = random.randint(1,40)
#     loan_amount = fake.pyfloat(positive=True, left_digits=5, right_digits=2,min_value=1.0)
#     customer_id = random.randint(1,200)
#     account_id = random.randint(1,200)
#     applied_date = fake.date_between(start_date = '-7300d', end_date = 'today')
#     employee_id = random.randint(1,200)

#     cur.execute("INSERT INTO loan_applications VALUES (%s, %s, %s, %s, %s, %s, %s,%s)", 
#                 (i,property_id, loan_type_id, loan_amount, customer_id, account_id, applied_date, employee_id))







# Generate and insert random mortgages info data
# for i in range(1, 1001):
#         property_id = i
#         property_address = fake.street_address()
#         property_type =  random.choice(['Single-Family','Condominium ','Townhouse','Apartment','Commercial ','Vacant Land','Mixed-Use Property'])
#         property_value = random.randint(10020000,1113000000)
#         year_built = fake.date_between(start_date='-30y', end_date='-1y').year
#         property_square_footage = random.randint(500, 5000)
#         property_owner_name = fake.name()
#         property_owner_phonenumber = random.randint(100000000, 9999999999)
#         property_owner_email_id = fake.email()
#         property_tax_rate = fake.random.uniform(0.5, 2.5)


#         cur.execute("INSERT INTO mortgages_info VALUES (%s, %s, %s, %s, %s, %s, %s , %s, %s, %s)", 
#         (property_id, property_address, property_type , property_value , year_built , property_square_footage ,
#                          property_owner_name , property_owner_phonenumber , property_owner_email_id , property_tax_rate))







# Generate and insert random refinancing_loan data
# for i in range(1, 1001):
#         refinancing_id = i
#         loan_id = random.randint(1,1000)
#         refinancing_type = random.choice(['Cashout','Fixed-rate'])
#         refinancing_loan_amount =  random.randint(10020000,1113000000)
#         originating_date = fake.date_between(start_date='-30y', end_date='-1y').year
#         closing_date = originating_date + 15
#         appraised_value = refinancing_loan_amount + 1000000


#         cur.execute("INSERT INTO refinancing_loan VALUES (%s, %s, %s, %s, %s, %s, %s)", 
#         (refinancing_id, loan_id ,refinancing_type, refinancing_loan_amount,originating_date,closing_date,appraised_value))




# Generate and insert random customer_satisfaction data
# for i in range(1, 1001):
#         satisfaction_id = i
#         application_id = random.randint(1,10000)
#         loan_id = random.randint(1,10000)
#         satisfaction_score = random.randint(1,5)
#         feedback = fake.paragraph()
#         feedback = textwrap.shorten(feedback, width=225)
#         date = fake.date_between(start_date='-2y', end_date='-1y')


#         cur.execute("INSERT INTO customer_satisfaction VALUES (%s, %s, %s, %s, %s, %s)", 
#         (satisfaction_id, application_id, loan_id,satisfaction_score,feedback,date))





# Generate data for transfer loan data 
# for i in range (1,101):
#     transfer_id = i
#     transfer_type = random.choice(['IN', 'OUT'])
#     loan_amount = random.randint(100000, 500000)
    
#     if transfer_type == 'IN':
       
#         other_interest_rate = round(random.uniform(4, 15), 2)
#         bank_interest_rate = round(random.uniform(4, other_interest_rate), 2)
#     else:
#         other_interest_rate = round(random.uniform(4, 15), 2)
#         bank_interest_rate = round(random.uniform(other_interest_rate, 15), 2)

#     cur.execute("INSERT INTO check_book VALUES (%s, %s, %s, %s, %s)", 
#                  (transfer_id, transfer_type, loan_amount, bank_interest_rate, other_interest_rate))    


# Generate data for card_details table         
# for i in range(1,40000):
        
#         card_type = random.choice(['DEBIT','CREDIT'])
#         if(card_type=='DEBIT'):
#             account_id = round(random.uniform(1,40000))
#             customer_id = round(random.uniform(1,30000))
#         else:
#             account_id = None
#             customer_id = round(random.uniform(1,30000))
#         card_number = fake.random_number(digits=16)
#         if(card_type=='DEBIT'):
#             card_limit = None
#         else:
#             card_limit = random.randint(2000,10000)
#         issued_date = fake.date_time_between(start_date = '-1095d', end_date = 'now')
#         if(card_type=='DEBIT'):
#             valid_date = issued_date + relativedelta(years=4)
#         else:
#             valid_date = issued_date + relativedelta(years=3)
#         if(card_type=='DEBIT'):
#             card_sub_type = random.choice(['UnionPay','American Express','Discover','MasterCard','Visa'])
#         else:
#             card_sub_type = random.choice(['Travel','Business','Reward','Shopping'])




# Generate data for customer exception data 
# for i in range (1,1001):
#         customer_id = random.randint(1,29000)
        # employee_id = random.randint(1,29000)
        # open_date = fake.date_between(start_date='-5000d', end_date='-1000d')
        # review_date =  open_date + timedelta(days=random.randint(500,600))
        # doc_status = random.choice(['Missing','Exception'])
        # description = random.choice(['Identity','CIP Exception'])
        # if(description == 'CIP Exception'):
        #     comments = random.choice(['Please re-sacn ID Verification','ID has Expired'])
        # else:
        #     comments = None 

#     cur.execute("INSERT INTO check_book VALUES (%s, %s, %s, %s, %s , %s , %s , %s)", 
#                  (exception_id, customer_id, employee_id, open_date, review_date , doc_status , description , comments))    






# Generate data for account exception data 
# for i in range (1,1001):
#         account_exception_id = i
#         account_id = random.randint(1,29000)
#         employee_id = random.randint(1,29000)
#         exception_date = fake.date_between(start_date='-5000d', end_date='-1000d')
#         exception_type = random.choice(['Overdrawn account','Unauthorized transaction', 'Account freeze'])
#         if(exception_type == 'Overdrawn account'):
#             description = 'Account balance dropped below zero'
#         elif(exception_type == 'Unauthorized transaction'):    
#             description = 'Suspicious transaction detected on the account'
#         else:
#             description = 'Account temporarily frozen due to suspected fraud'    


#         exception_status = random.choice(['In progress','Resolved','Under investigation'])

#     cur.execute("INSERT INTO check_book VALUES (%s, %s, %s, %s, %s , %s , %s )", 
#                  (account_exception_id, account_id, employee_id, exception_date , exception_status , exception_type , description ))    




# Commit changes and close connection
cur.execute("COMMIT")
cur.close()
conn.close()
