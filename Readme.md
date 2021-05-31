# DataEngExam
 DataEngExam is Data Engineering Exam 
 ## Description
![image](https://user-images.githubusercontent.com/40566685/120214906-4f1e4e80-c25f-11eb-9c7a-0199295f651c.png)
It id the construction data pipeline as the picture 
 - Get data from PostgreSQL Hosting
 - Create DataLake by Google Cloud Storages  
 - Passing PostgreSQL to DataLake
 - Create Data Warehouse by Google BigQuery 
 - Passing data in DataLake to Data Warehouse
## Usage
By default, docker-airflow runs Airflow with  **SequentialExecutor**  :
```
docker run -d -p 8080:8080 <docker_image_name> webserver
```
If you want to run another executor, use the other docker-compose.yml files provided in this repository.
```
docker-compose up -d
```
## Set Airflow
### Set Variable
<img width="1792" alt="Screen Shot 2564-05-31 at 21 49 10" src="https://user-images.githubusercontent.com/40566685/120215135-93115380-c25f-11eb-98e7-fc41a787d964.png">
POSTGRESQL_TABLE is Name of table in PostgreSQL , There is a ',' conflict between table names.
 - Example
 ```
## Table name is users and user_log
POSTGRESQL_TABLE = "users,user_log"
```
POSTGRESQL_TABLE_COLUMN_NAME is Name of column table in PostgreSQL . There is a ',' conflict between column table names and There is a '/' conflict between table names
 - Example
```
## User table have created_at , updated_at , id , first_name, 
last_name and User_log have created_at , updated_at , id , user_id , action , status

POSTGRESQL_TABLE_COLUMN_NAME = "created_at,updated_at,id,first_name,last_name/created_at,updated_at,id,user_id,action,status"
```
 ### Set GCP Connections
 edit Connections name ' google_cloud_default'
 <img width="1792" alt="Screen Shot 2564-05-31 at 22 04 48" src="https://user-images.githubusercontent.com/40566685/120215155-99073480-c25f-11eb-8df4-5d95daeab4a9.png">


## Problem
My Airflow cannot connect BigQuery even though my keys in 'Keyfile Path'  have access BigQuery Owner Role.