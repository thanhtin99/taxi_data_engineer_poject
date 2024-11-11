# Taxi_data_engineer_poject
## Introduction
The goal of the project is to create a pipeline to store, manage and analyze Uber taxi data from open source tools such as Spark, Delta Lake, PostgreSQL, Docker và Apache Superset and GCP Storage.
## Architecture 

![image](https://github.com/user-attachments/assets/ced52026-e8c3-47f0-a106-9bbbc42e1b87)

![image](https://github.com/user-attachments/assets/ea5f9ff4-6071-4191-9d86-da89402586cd)

## Dataset Used
TLC Trip Record Data
Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. 
Website - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Data model

![image](https://github.com/user-attachments/assets/bb98f426-3d7a-4e3d-b0ed-bb6b751d1079)

## Technology Used
- MinIO is an object storage service similar to Amazon S3. It is used to store and manage large data efficiently.
- PostgreSQL is a powerful open source relational database management system. It is used to store and manage structured data.
- Superset is an open source BI (Business Intelligence) platform for data visualization and exploration.
- Spark is a powerful big data processing engine, used to process and analyze data at large scale.
- Built all on Docker
- Google Cloud Platform: Google Storage, Compute Instance, BigQuery, Looker Studio


## Result
### Data is ETL and fully stored in PostgreSQL, Minio, Google BigQuery
![image](https://github.com/user-attachments/assets/7689128b-f2b7-4232-b79a-bd46a3a8b470)

![image](https://github.com/user-attachments/assets/11deb4c0-4fcb-4236-8575-1c66c39d5669)


### Access to Superset in http://localhost:8088/ to build visualization

![image](https://github.com/user-attachments/assets/07273568-e6e8-4917-bc03-db77cb34bccc)



