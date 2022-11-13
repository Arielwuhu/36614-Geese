"""
Python script to clean, process, and insert the quality data
into our SQL data tables.

Enter branch: git checkout Donny
Exit branch: git checkout main

Algo:
**Create data tables in ssh psql**
1. Load Data into pandas
2. Clean data as pandas df
3. Insert into sql data table
"""

import pandas as pd


hhs_df = pd.read_csv("")