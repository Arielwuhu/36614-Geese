
# 36614-Geese

![alt text](https://media.istockphoto.com/id/1277343373/vector/banner-with-isolated-on-white-background-flying-migratory-birds.jpg?s=612x612&w=0&k=20&c=gE3-xy7SaeHd4tygjH81YOcchZn3a8g5AkV7MHBUwJ4=)

Group Geese's data pipeline project.

This repository contains 

1. SQL Data Table Schema

    - Hospital_Info
    - Hospital_Coord
    - Hospital_Stat
    - Rating
    
2. Data

    - Health and Human Services (HHS) Data
    - Hospital Quality Data

3. Cleaning scripts to input our data into the SQL
data tables where our data is being stored.

    - load-hhs.py
    - load-quality.py


papermill weekly-report.ipynb 2022-10-21-report.ipynb -p collection_week 2022-10-21
jupyter nbconvert --no-input --to html 2022-10-21-report.ipynb
