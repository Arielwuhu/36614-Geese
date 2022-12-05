/* 
Team Name: Geese
Members: Donald Dinerman, Theo Wang, Ariel (Yicheng) Wang
*/

/* 1: Hospital_Info(hospital_pk, name, address, city, state, zip_code, fips_code, ownership, emergency) */
CREATE TABLE Hospital_Info(
	hospital_pk varchar(255) UNIQUE PRIMARY KEY,
	name text, 
	address text,
	city text,
	state char(2),
	zip_code varchar(5),
    county text,
	ownership text,
	emergency boolean DEFAULT false);

/* 2: Hospital_Coord(hospital_pk, longitude, latitude, fips_code) */
CREATE TABLE Hospital_Coord(
      hospital_pk varchar(255) UNIQUE PRIMARY KEY,
      longitude numeric,
      latitude numeric,
      fips_code varchar(5));

/* 3: Hospital_Stat(hospital_pk, time_week,  all_adult_hospital_beds, all_pediatric_inpatient_beds, 
                    all_adult_hospital_inpatient_bed_occupied, all_pediatric_inpatient_bed_occupied, total_icu_beds, 
                    icu_beds_used, inpatient_beds_used_covid, staffed_adult_icu_patients_confirmed_covid) */
CREATE TABLE Hospital_Stat(
	hospital_pk varchar(255),
	collection_week date CHECK(collection_week <= current_date),
	all_adult_hospital_beds_7_day_avg numeric CHECK(all_adult_hospital_beds_7_day_avg >= 0),
	all_pediatric_inpatient_beds_7_day_avg numeric CHECK(all_pediatric_inpatient_beds_7_day_avg >= 0),
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage numeric CHECK(all_adult_hospital_inpatient_bed_occupied_7_day_coverage >= 0), 
	all_pediatric_inpatient_bed_occupied_7_day_avg numeric  CHECK(all_pediatric_inpatient_bed_occupied_7_day_avg >= 0),
	total_icu_beds_7_day_avg numeric CHECK(total_icu_beds_7_day_avg >= 0),
	icu_beds_used_7_day_avg numeric CHECK(icu_beds_used_7_day_avg >= 0),
	inpatient_beds_used_covid_7_day_avg numeric CHECK(inpatient_beds_used_covid_7_day_avg >= 0), 
	staffed_adult_icu_patients_confirmed_covid_7_day_avg numeric CHECK(staffed_adult_icu_patients_confirmed_covid_7_day_avg >= 0),
	CONSTRAINT hos_week UNIQUE (hospital_pk, collection_week));

/* 4: Rating_Time(hospital_pk, rating_year, rating)*/
CREATE TABLE Rating(
	hospital_pk varchar(255) REFERENCES Hospital_Info,
    rating int CHECK (rating >= 0),
	rating_year date CHECK (rating_year <= current_date));

/*
To ensure that we minimize the redundant information in our data tables we normalized our database schema to a 
reasonable extent. For instance, we use hospital_pk as the primary and foreign keys for many of our tables because they contain
different information. So Rating_Time and Hospital_Stat contains time series data that would be exceedingly repeated if it
was contained in our Hospital_Info data table for instance. We also created a Hospital_Coord data table to contain geolocation 
information for each hospital that relates to the Hospital_Info table with the hospital_pk foreign key. Lastly, our Hospital_Info 
data table contains basic information about the hospital such as the name, adress, city, ownership type, and whether they offer 
emergency services. This information is relatively static which shouldn't change over time.
*/