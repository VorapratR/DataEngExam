# DataEngExam
 DataEngExam is Data Engineering Exam 
 ## Description
 ![ScreenShot](https://sv1.picz.in.th/images/2021/05/31/PHwJ4z.png?raw=true) 
 <img src="https://sv1.picz.in.th/images/2021/05/31/PHwJ4z.png?raw=true" >
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
![ScreenShot](https://sv1.picz.in.th/images/2021/05/31/PHKNBa.png)
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
 ![ScreenShot](https://sv1.picz.in.th/images/2021/05/31/PHZBLQ.png)

## Problem
My Airflow cannot connect BigQuery even though my keys in 'Keyfile Path'  have access BigQuery Owner Role.