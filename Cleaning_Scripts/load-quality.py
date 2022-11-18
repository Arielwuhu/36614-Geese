"""
Python script to clean, process, and insert the quality data
into our SQL data tables.

Branch modification.
"""

"""
Python script to clean, process, and insert the quality data
into our SQL data tables.

Branch modification.
"""

import numpy as np
import pandas as pd
import sys
import datetime
import psycopg

'''1. Read data'''
quality = pd.read_csv("../Data/Quality/" + sys.argv[2])


'''2. Data Preprocessing'''
# Replace 'Not Available' value to NaN
info1 = info1.replace('Not Available', np.nan)
## Insert date column as python date object
date = sys.argv[1]
quality['Rating year'] = date


'''3. Load data into psql'''
# Connect to psql server
conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname="yicheng6",
    user="yicheng6", password=""
)
cur = conn.cursor()


'''3.1 Hospital_Info(hospital_pk, name, address, city, state, zip_code, 
ownership, emergency)'''
# Create a seperate table containing useful columns
info_table = info1.loc[:,['Facility ID', 'Facility Name', 'Address', 
'City',\
                          'State', 'ZIP Code', 'Hospital Ownership', 
'Emergency Services']]

# Change the data type
info_table["Facility ID"] = info_table["Facility ID"].astype('string')
info_table["Facility Name"] = info_table["Facility Name"].astype('string')
info_table["Address"] = info_table["Address"].astype('string')
info_table["City"] = info_table["City"].astype('string')
info_table["State"] = info_table["State"].astype('string')
info_table["ZIP Code"] = info_table["ZIP Code"].astype('string')
info_table["Hospital Ownership"] = info_table["Hospital 
Ownership"].astype('string')
info_table["Emergency Services"] = info_table["Emergency 
Services"].astype('bool')

# Container to record insert failed row
key = ['Facility ID', 'Facility Name', 'Address', 'City',\
       'State', 'ZIP Code', 'Hospital Ownership', 'Emergency Services']
df_error = pd.DataFrame(columns=key)

num_rows_inserted = 0

# make a new transaction
with conn.transaction():
    
    for index, row in info_table.iterrows():
        try:
            # make a new SAVEPOINT -- like a save in a video game
            cur.execute("SAVEPOINT save1")
            with conn.transaction():  
                # now insert columns into the data
                insert = ("INSERT INTO Hospital_Info "
                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
                cur.execute(insert, tuple(row))
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes 
back to
            # the last savepoint upon exiting the `with` block
            print("insert failed in row " + str(index))
            df_error = pd.concat([df_error, row])

            # add additional logging, error handling here
        else:
            # no exception happened, so we continue without reverting the 
savepoint
            num_rows_inserted += 1
            
    print('Inserted ' + str(num_rows_inserted) + ' rows for Hospital_Info 
table.')
    df_error.to_csv("Error_row_hospitalinfo.csv", index = False)

# now we commit the entire transaction
conn.commit()


'''3.2 Rating(hospital_pk, rating_year, rating)'''
# Create a seperate table containing useful columns
rate_table = info1.loc[:,["Facility ID","Hospital overall rating", "Rating 
year"]]
rate_table["Facility ID"] = rate_table["Facility ID"].astype('string')
rate_table["Hospital overall rating"] = rate_table["Hospital overall 
rating"].astype('Int64')
rate_table['Rating year'] = pd.to_datetime(rate_table['Rating year'], 
format="%Y-%m-%d")

# Container to record insert failed row
key = ["hospital_pk","Hospital overall rating", "Rating year"]
df_error = pd.DataFrame(columns=key)

num_rows_inserted = 0

# make a new transaction
with conn.transaction():
    
    for index, row in rate_table.iterrows():
        try:
            # make a new SAVEPOINT -- like a save in a video game
            cur.execute("SAVEPOINT save1")
            with conn.transaction():  
                # now insert  (hospital_pk, rating_year, rating) into the 
data
                # since the rating will update several times a year, we 
have to keep the latest one on the table
                insert = ("INSERT INTO Rating "
                          "VALUES (%(hospital_pk)s, %(rating)s, 
%(rating_year)s)"
                          "ON CONFLICT (hospital_pk) DO UPDATE "
                          "SET rating = %(rating)s, rating_year = 
%(rating_year)s")
                
                cur.execute(insert, {
                    "hospital_pk": row['Facility ID'],
                    "rating": row['Hospital overall rating'],
                    "rating_year": row['Rating year']
                })
        except Exception as e:
            # if an exception/error happens in this block, Postgres goes 
back to
            # the last savepoint upon exiting the `with` block
            print("insert failed in row " + str(index))
            df_error = pd.concat([df_error, row])

            # add additional logging, error handling here
        else:
            # no exception happened, so we continue without reverting the 
savepoint
            num_rows_inserted += 1

    print('Inserted ' + str(num_rows_inserted) + ' rows for Rating_Time 
table.')
    df_error.to_csv("Error_row_ratingtime.csv", index = False)

# now we commit the entire transaction
conn.commit()
