"""
Python script to clean, process, and insert the HHS data
into our SQL data tables.
"""
import pandas as pd
import numpy as np
import psycopg
import sys
import credentials


# Load data
hhs = pd.read_csv("../Data/HHS/" + sys.argv[1])
# Convert -999999 to NaN
hhs.replace(-999999, None, inplace=True)
# Parse dates into Python date objects
hhs['collection_week'] = pd.to_datetime(hhs['collection_week']).dt.date
# Drop hospital ID which do not meet the standard format
hhs.drop(hhs[hhs['hospital_pk'].str.len() != 6].index)


# Extract latitude values
hhs['latitude'] = hhs['geocoded_hospital_address'].str.replace(
    'POINT', '', regex=True).str.replace('(', '', regex=True).str.replace(
    ')', '', regex=True).str.strip().str.split(' ').apply(
    lambda d: d if isinstance(d, list) else [None, None])
hhs['latitude'] = hhs['latitude'].apply(lambda d: d[0])


# Extract longitude values
hhs['longitude'] = hhs['geocoded_hospital_address'].str.replace(
    'POINT', '', regex=True).str.replace('(', '', regex=True).str.replace(
    ')', '', regex=True).str.strip().str.split(' ').apply(
    lambda d: d if isinstance(d, list) else [None, None])
hhs['longitude'] = hhs['longitude'].apply(lambda d: d[1])


# Replace all the NaN to None
hhs.replace(np.NaN, None, inplace=True)


# Key of Hospital_Stat
key = ["hospital_pk", "collection_week",
       "all_adult_hospital_beds_7_day_avg",
       "all_pediatric_inpatient_beds_7_day_avg",
       "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
       "all_pediatric_inpatient_bed_occupied_7_day_avg",
       "total_icu_beds_7_day_avg", "icu_beds_used_7_day_avg",
       "inpatient_beds_used_covid_7_day_avg",
       "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
hhs_insert = hhs.loc[:, key]


# Key of Hospital_Coord
key_coor = ["hospital_pk", "longitude", "latitude", "fips_code"]
hhs_coor = hhs.loc[:, key_coor]


conn = psycopg.connect(
    host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
    user=credentials.DB_USER, password=credentials.DB_PASSWORD
)
cur = conn.cursor()


# # This is to truncate the table
# with conn.transaction():
#     cur.execute("TRUNCATE Hospital_Stat")
#     cur.execute("TRUNCATE Hospital_Coord")


# Insert into Hospital_Stat
with conn.transaction():

    num_rows_inserted = 0
    error_index = []
    for index, row in hhs_insert.iterrows():
        try:
            # make a new SAVEPOINT
            cur.execute("SAVEPOINT save1")
            with conn.transaction():
                # now insert the data
                insert = ("INSERT INTO Hospital_Stat "
                          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                          "ON CONFLICT (hos_week) DO NOTHING")
                cur.execute(insert, tuple(row))

        except Exception as e:
            # if an exception/error happens in this block,
            # Postgres goes back to the last savepoint upon
            # exiting the `with` block and print error
            print("insert failed in row " + str(index) + ":", e)
            error_index.append(index)

        else:
            num_rows_inserted += 1
    print("A total of " + str(num_rows_inserted) + " are inserted")
    df_error = hhs_insert.iloc[error_index]
    df_error.to_csv("Error_row_stat.csv", index=False)
# # now we commit the entire transaction
conn.commit()


# Insert into Hospital_Coord
with conn.transaction():

    num_rows_inserted = 0
    error_index = []
    for index, row in hhs_coor.iterrows():
        try:
            # make a new SAVEPOINT
            cur.execute("SAVEPOINT save2")
            with conn.transaction():
                # now insert the data and handle conflict
                insert = ("INSERT INTO Hospital_Coord "
                          "VALUES(%(hospital_pk)s, %(longitude)s,\
                                  %(latitude)s, %(fips_code)s) "
                          "ON CONFLICT (hospital_pk) DO UPDATE "
                          "SET longitude = %(longitude)s,\
                               latitude = %(latitude)s,\
                               fips_code = %(fips_code)s")
                cur.execute(insert, {
                    "hospital_pk": row['hospital_pk'],
                    "longitude": row['longitude'],
                    "latitude": row['latitude'],
                    "fips_code": row['fips_code']
                })

        except Exception as e:
            # if an exception/error happens in this block,
            # Postgres goes back to the last savepoint upon
            # exiting the `with` block and print error
            print("insert failed in row " + str(index) + ":", e)
            error_index.append(index)

        else:
            num_rows_inserted += 1

    print("A total of " + str(num_rows_inserted) + " are inserted")
    df_error = hhs_coor.iloc[error_index]
    df_error.to_csv("Error_row_coord.csv", index=False)
# now we commit the entire transaction
conn.commit()
