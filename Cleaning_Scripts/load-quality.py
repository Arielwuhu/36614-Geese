"""
Python script to clean, process, and insert the HHS data
into our SQL data tables.

Enter branch: git checkout Donny
Exit branch: git checkout main

# Connect to server
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)
"""
import pandas as pd
import numpy as np
import psycopg
import sys
import credentials

# hhs_df = pd.read_csv("Data/HHS/2022-09-23-hhs-data.csv")
hhs = pd.read_csv(sys.argv[1]) # hhs_df = pd.read_csv("Data/HHS/"+sys.argv[1])
# Convert -999999 to NaN
hhs.replace(-999999, np.NaN, inplace=True)
# Parse dates into Python date objects
hhs['collection_week'] = pd.to_datetime(hhs['collection_week']).dt.date
# Drop ID not satisfying standard format
hhs.drop(hhs[hhs['hospital_pk'].str.len() != 6].index)

# Extract latitude values
hhs['latitude'] = hhs['geocoded_hospital_address'].str.replace('POINT','',regex=True) \
    .str.replace('(','',regex=True).str.replace(')','',regex=True).str.strip().str.split(' ') \
    .apply(lambda d: d if isinstance(d, list) else [np.NaN,np.NaN])
hhs['latitude'].apply(lambda d: d[0])

# Extract longitude values
hhs['longitude'] = hhs['geocoded_hospital_address'].str.replace('POINT','',regex=True) \
    .str.replace('(','',regex=True).str.replace(')','',regex=True).str.strip().str.split(' ') \
    .apply(lambda d: d if isinstance(d, list) else [np.NaN,np.NaN])
hhs['longitude'].apply(lambda d: d[1])


key_numeric = ["all_adult_hospital_beds_7_day_avg",\
    "all_pediatric_inpatient_beds_7_day_avg", "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",\
        "all_pediatric_inpatient_bed_occupied_7_day_avg", "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",\
            "inpatient_beds_used_covid_7_day_avg", "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
hhs_insert = hhs.loc[:,key_numeric]
hhs[key_numeric] = hhs[key_numeric].apply(pd.to_numeric, errors='coerce')

key = ["hospital_pk", "collection_week","all_adult_hospital_beds_7_day_avg",\
    "all_pediatric_inpatient_beds_7_day_avg", "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",\
        "all_pediatric_inpatient_bed_occupied_7_day_avg", "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",\
            "inpatient_beds_used_covid_7_day_avg", "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
hhs_insert = hhs.loc[:,key]

# Connect to server
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)

# conn = psycopg.connect(
#     host="sculptor.stat.cmu.edu", dbname="xiyaowan",
#     user="xiyaowan", password="ooXee7ad9"
# )

cur = conn.cursor()

df_error = pd.DataFrame(columns=key)
num_rows_inserted = 0

# This is to truncate the table
# with conn.transaction():
#     cur.execute("TRUNCATE Hospital_Stat ")

with conn.transaction():
    for index, row in hhs_insert.iterrows():
        try:
            # make a new SAVEPOINT -- like a save in a video game
            cur.execute("SAVEPOINT save1")
            with conn.transaction():  
                # now insert the data
                insert = ("INSERT INTO Hospital_Stat "
                        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                cur.execute(insert, tuple(row))


        except Exception as e:
            # if an exception/error happens in this block, Postgres goes back to
            # the last savepoint upon exiting the `with` block
            print("insert failed in row " + str(index))
            df_error = pd.concat(df_error, row)

            # add additional logging, error handling here
        else:
            # no exception happened, so we continue without reverting the savepoint
            num_rows_inserted += 1
    print(num_rows_inserted)
    df_error.to_csv("Error_row.csv", index = False)
# now we commit the entire transaction
# conn.commit()
