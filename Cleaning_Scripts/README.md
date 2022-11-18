
# Data Pipeline Input Scripts

This folder contains the python scripts to clean the raw data and
input them into our respective SQL data tables.

Below we specify how to use our scripts to load the 
HHS and Quality data.

## HHS Data

The Health and Human Services (HHS) Data is used to populate 
the following SQL data tables: 

    1. Hospital_Coord
    2. Hospital_Stat

This script is designed to input the data through a command line.
The data file's path is assumed to be in the following folders 
"../Data/HHS/".

Below is an example command line call (the HHS csv
is a placeholder for any HHS csv file):

    1. python load-hhs.py ['HHS csv file']
        - python load-hhs.py 2022-01-04-hhs-data.csv

## Quality Data

The Hospital Quality Data is used to populate the following SQL 
data tables:

    1. Hospital_Info
    2. Rating_Time

This script is designed to input the data through a command line.
The data file's path is assumed to be in the following folders 
"../Data/Quality/".

Below is an example command line call (the Quality csv
is a placeholder for any Quality csv file, the Data date is
a placeholder for the respective date of the Quality data):

    1. python load-quality.py ['Data date'] ['Quality csv file']
        - python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv