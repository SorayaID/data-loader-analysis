import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import calendar

# Load companies.json, jobs.json and locations.csv into DataFrames
companies_data = pd.read_json('data/companies.json')
jobs_data = pd.read_json('data/jobs.json')
locations_data = pd.read_csv('data/locations.csv')

#Let's have a look at the data in these data frames:
print(companies_data.head())
print(companies_data.info())
print(jobs_data.head())
print(jobs_data.info())
print(locations_data.head())
print(locations_data.info())

# *************************************************** Data quality checks ***************************************************

# The Establishment Date is originally a Unix timestamp, and so let's convert it to a regular timestamp
companies_data['Establishment Date'] = pd.to_datetime(companies_data['Establishment Date'], unit='ms', origin='unix')
print("Establishment Date has been converted to a regular timestamp.\n")

# Let's make sure that the values of the "state" column are all correct
distinct_states = jobs_data['state'].value_counts().index.tolist()
print(distinct_states)

# Some of th state values seem to be wrong, so let's correct them:
jobs_data['state'] = jobs_data['state'].replace({'cancelled': 'canceled', 'osted': 'posted'})

# Let's check the changes
print(jobs_data['state'].unique())
print("The state column in jobs_data has been corrected.\n")

# Let's also check for rows where the "zip_code" column is null
null_zip_code_rows = jobs_data['zip'].isnull().sum()
print("Total rows with null zip in jobs dataframe:", null_zip_code_rows)

#Let's define a funtion to check if there are any duplicated rows
def check_duplicates(dataframe):
    # Check for duplicates and return the duplicated rows if any are found
    duplicates = dataframe[dataframe.duplicated(keep=False)]
    if not duplicates.empty:
        print("Duplicates found:")
        print(duplicates)
    else:
        print("No duplicates found.")
    
#Now let's use this funtion to detecte duplicated rows:
print("\nChecking duplicated rows for jobs_data...")
check_duplicates(jobs_data)

print("\nChecking duplicated rows for locations_data...")
check_duplicates(locations_data)

print("\nChecking duplicated rows for companies_data...")
check_duplicates(companies_data)

#Duplicated rows have been found in jobs_data, and so let's drop the duplicated rows:
jobs_data = jobs_data.drop_duplicates(keep='first')
print("\nDuplicated rows have been dropped in the dataframe jobs_data.")

#*********************************************************************************************************************************

#1. What location has the most jobs that are either posted or expired?
# Join the "jobs" and "locations" DataFrames based on the condition zip=zip_code
joined_data = jobs_data.merge(locations_data, left_on='zip', right_on='zip_code', how='inner')
#print(joined_data)

# Filter for jobs that are either "posted" or "expired"
joined_data = joined_data[joined_data['state'].isin(['posted', 'expired'])]
#print(joined_data)

# Group by location and count the number of jobs in each location
location_job_counts = joined_data['location'].value_counts().reset_index()
location_job_counts.columns = ['Location', 'Job Count']

# Find the location with the highest job count
max_job_location = location_job_counts.loc[location_job_counts['Job Count'].idxmax()]

print("Location with the most jobs (posted or expired):")
print(max_job_location)


#2. What month had the most cancelled jobs? (Just the month without the year)

jobs_data['posted_at_month'] = jobs_data['posted_at'].dt.month
canceled_jobs = jobs_data[jobs_data['state'] == 'canceled']

# Group by month and count canceled jobs
canceled_jobs_by_month = canceled_jobs.groupby('posted_at_month')['state'].count()

# Get the month with the highest count
most_canceled_month = canceled_jobs_by_month.idxmax()

print("Month with the most canceled jobs:", calendar.month_name[most_canceled_month])

#2. What month had the most cancelled jobs? (Month and year)
jobs_data['posted_at_month'] = jobs_data['posted_at'].dt.month
jobs_data['posted_at_year'] = jobs_data['posted_at'].dt.year
canceled_jobs = jobs_data[jobs_data['state'] == 'canceled']

# Group by month and count canceled jobs
canceled_jobs_by_month_year = canceled_jobs.groupby(['posted_at_month', 'posted_at_year'])['state'].count()

# Get the month with the highest count
most_canceled_month_year = canceled_jobs_by_month_year.idxmax()

print("Month and year with the most canceled jobs:", most_canceled_month_year)

#3. Which company has the highest ratio of posted jobs to employee count?

# We already know the employee count from the data provided, let's count the jobs count per company
posted_jobs = jobs_data[jobs_data['state'] == 'posted']
posted_jobs = posted_jobs.groupby('company_id').size().reset_index(name='jobs_count')
# Set 'Company ID' as the index
posted_jobs.set_index('company_id', inplace=True)

posted_jobs_employee = posted_jobs.merge(companies_data, left_on='company_id', right_on='Company ID', how='inner')
posted_jobs_employee['ratio_posted_jobs_employee'] = posted_jobs_employee['jobs_count'] / posted_jobs_employee['Number of Employees']

sorted_posted_jobs_employee = posted_jobs_employee.sort_values(by='ratio_posted_jobs_employee', ascending=False)

# Get the company with the highest ratio
highest_ratio_company = sorted_posted_jobs_employee.iloc[0]

# Print the company name and the highest ratio
print("Company with the highest ratio:")
print("Company Name:", highest_ratio_company['Company Name'])
print("Ratio:", highest_ratio_company['ratio_posted_jobs_employee'])
