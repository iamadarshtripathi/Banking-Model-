create or replace stream staging.st_transactions
on table staging.transactions;


create or replace stream staging.st_accounts
on table staging.accounts;


create or replace stream staging.st_customers
on table staging.customers;


create or replace stream staging.st_loan_details
on table staging.loan_details;


create or replace stream staging.st_transaction_types
on table staging.transaction_types;


create or replace stream staging.st_loan_types
on table staging.loan_types;


create or replace stream staging.st_account_types
on table staging.account_types;



create or replace stream staging.st_transaction_formats
on table staging.transaction_formats;

create or replace stream staging.st_account_types
on table staging.account_types;


create or replace stream staging.st_card_details
on table staging.card_details;


create or replace stream staging.st_branches
on table staging.branches;

create or replace stream staging.st_employees
on table staging.employees;


create or replace stream staging.st_check_book
on table staging.check_book;

create or replace stream staging.st_fixed_deposit
on table staging.fixed_deposit;


create or replace stream staging.st_transfer_loan_data
on table staging.transfer_loan_data;


create or replace stream staging.st_auto_info
on table staging.auto_info;

create or replace stream staging.st_loan_applications
on table staging.loan_applications;

create or replace stream staging.st_map_auto_loan
on table staging.map_auto_loan;

create or replace stream staging.st_mortgages_info
on table staging.mortgages_info;

create or replace stream staging.st_refinancing_loan
on table staging.refinancing_loan;

create or replace stream staging.st_customer_satisfaction
on table staging.customer_satisfaction;

create or replace stream staging.st_account_applications
on table staging.account_applications;


create or replace stream staging.ST_CIF_EXCEPTION
on table staging.CIF_EXCEPTION;

create or replace stream staging.st_account_exception
on table staging.account_exception;

create or replace stream staging.st_fixed_asset
on table staging.fixed_asset;

create or replace stream staging.st_capital_expenditure
on table staging.capital_expenditure;

create or replace stream staging.st_departments
on table staging.departments;

create or replace stream staging.st_expense
on table staging.Expense;

create or replace stream staging.st_securities
on table staging.securities;

create or replace stream staging.st_retail_performance
on table staging.retail_performance;

create or replace stream staging.st_bank_debt
on table staging.bank_debt;

create or replace stream staging.st_non_performing_loan
on table staging.non_performing_loan;

create or replace stream staging.st_credit_recovery
on table staging.credit_recovery;

create or replace stream staging.st_fund_recovery
on table staging.fund_recovery;

create or replace stream staging.st_uninsured_deposits
on table staging.uninsured_deposits;

