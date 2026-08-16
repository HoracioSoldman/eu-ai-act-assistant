### Running the project
#### Airflow
* go to the project folder
* run `docker compose up --build -d`
* go to the http://localhost:8080/dags to see the dags
* if nothing shows up there, run this to fix it `docker compose exec airflow-scheduler airflow dags reserialize`

#### Minio
* go to http://localhost:9001/
* use the default following credentials, username: minioadmin; password: minioadmin
* 
