"""
Python script to clean, process, and insert the quality data
into our SQL data tables.

Branch modification.
"""

import numpy as np
import pandas as pd
import sys
import psycopg
import credentials


'''1. Read data'''
quality = pd.read_csv("../Data/Quality/" + sys.argv[2])


'''2. Data Preprocessing'''
# Replace 'Not Available' and 'NaN' value to None
quality = quality.replace('Not Available', None)
quality = quality.replace(np.nan, None)
# Insert date column as python date object
date = sys.argv[1]
date = date.split('-')[0]
quality['Rating year'] = date


'''3. Load data into psql'''
# Connect to psql server
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)
cur = conn.cursor()


'''3.1 Hospital_Info(hospital_pk, name, address, city, state, zip_code,
ownership, emergency)'''
# Create a seperate table containing useful columns
info_table = quality.loc[:, ['Facility ID', 'Facility Name', 'Address',
                             'City', 'State', 'ZIP Code',
                             'Hospital Ownership', 'Emergency Services']]

# Change the data type
info_table["Facility ID"] = info_table["Facility ID"].astype('string')
info_table["Facility Name"] = info_table["Facility Name"].astype('string')
info_table["Address"] = info_table["Address"].astype('string')
info_table["City"] = info_table["City"].astype('string')
info_table["State"] = info_table["State"].astype('string')
info_table["ZIP Code"] = info_table["ZIP Code"].astype('string')
info_table["Hospital Ownership"] = info_table[
                                   "Hospital Ownership"].astype('string')
info_table["Emergency Services"] = info_table[
                                   "Emergency Services"].astype('bool')

# Container to record insert failed row
key = ['Facility ID', 'Facility Name',
       'Address', 'City', 'State', 'ZIP Code',
       'Hospital Ownership', 'Emergency Services']
df_error = pd.DataFrame(columns=key)

num_rows_inserted = 0

# make a new transaction
with conn.transaction():

    for index, row in info_table.iterrows():
        try:
            # make a new SAVEPOINT
            cur.execute("SAVEPOINT save1")
            with conn.transaction():  
                # now insert  (hospital_pk, rating_year, rating) into the data
                insert = ("INSERT INTO Hospital_Info "
                          "VALUES(%(hospital_pk)s, %(Facility Name)s,\
                                  %(Address)s, %(City)s, %(State)s, %(ZIP Code)s,\
                                  %(Hospital Ownership)s, %(Emergency Services)s) "
                          "ON CONFLICT (hospital_pk) DO UPDATE "
                          "SET Facility Name = %(Facility Name)s,\
                               Address = %(Address)s,\
                               City = %(City)s,\
                               State = %(State)s,\
                               ZIP Code = %(ZIP Code)s,\
                               Hospital Ownership = %(Hospital Ownership)s,\
                               Emergency Services = %(Emergency Services)s")
                cur.execute(insert, {
                    "hospital_pk": row['Facility ID'],
                    "Facility Name": row['Facility Name'],
                    "Address": row['Address'],
                    "City": row['City'],
                    "State": row['State'],
                    "ZIP Code": row['ZIP Code'],
                    "Hospital Ownership": row['Hospital Ownership'],
                    "Emergency Services": row['Emergency Services'],
                })
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes
            # back to the last savepoint upon exiting the `with` block
            print("insert failed in row " + str(index) + ":", e)
            df_error = pd.concat([df_error, row])

        else:
            num_rows_inserted += 1

    print('Inserted ' + str(num_rows_inserted) + ' rows for '
          'Rating_Time table.')
    df_error.to_csv("Error_row_hospitalinfo.csv", index=False)

# now we commit the entire transaction
conn.commit()


'''3.2 Rating(hospital_pk, rating_year, rating)'''
# Create a seperate table containing useful columns
rate_table = quality.loc[:, ["Facility ID",
                             "Hospital overall rating",
                             "Rating year"]]

# Container to record insert failed row
key = ["hospital_pk",
       "Hospital overall rating",
       "Rating year"]
df_error = pd.DataFrame(columns=key)

num_rows_inserted = 0

# make a new transaction
with conn.transaction():

    for index, row in rate_table.iterrows():
        try:
            # make a new SAVEPOINT
            cur.execute("SAVEPOINT save2")
            with conn.transaction():
                # now insert  (hospital_pk, rating_year, rating) into the data
                insert = ("INSERT INTO Rating "
                          "VALUES (%(hospital_pk)s, %(rating_year)s,\
                                  %(rating)s)"
                          "ON CONFLICT (rating_year) DO UPDATE "
                          "SET hospital_pk = %(hospital_pk)s,\
                               rating = %(rating)s")

                cur.execute(insert, {
                    "hospital_pk": row['Facility ID'],
                    "rating_year": row['rating_year'],
                    "rating": row['rating']
                })
        except Exception as e:
            # if an exception/error happens in this block, Postgres
            # goes back to the last savepoint upon exiting the `with` block
            print("insert failed in row " + str(index) + ":", e)
            df_error = pd.concat([df_error, row])

        else:
            num_rows_inserted += 1

    print('Inserted ' + str(num_rows_inserted) + ' rows for '
          'Rating_Time table.')
    df_error.to_csv("Error_row_ratingtime.csv", index=False)

# now we commit the entire transaction
conn.commit()
