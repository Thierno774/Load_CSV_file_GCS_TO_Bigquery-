# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


## create the parameters
BUCKET_NAME = ""
FOLDER_NAME_BUCKET = f"{BUCKET_NAME}/dataset/GreenHouseDataset"
DESTINATION_BQ_TABLE = ""

DATASET_NAME_CSV = "dataset/GreenHouseDataset /dataGreenHouse.csv"



# Default_args 

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    "start_date": yesterday, 
    "email_on_failure": False, 
    "email_on_retry": False, 
    "retries": 1, 
    "retry_delay": timedelta(minutes=5)
}

# DAG definition
with DAG(dag_id ="GSC_to_BG_and_AGG", 
        catchup = False, 
        default_args = default_args, 
        schedule_interval = timedelta(days=1)
) as dag: 
    # Dummy start task 
    start = DummyOperator(task_id = "start", 
                    dag = dag, )

    # Chargement du fichier GCS vers BigQuery 
    load_csv_to_bq = GoogleCloudStorageToBigQueryOperator(
                    task_id = "gcs_to_bg_load", 
                    bucket = BUCKET_NAME, 
                    source_objects = [DATASET_NAME_CSV], 
                    destination_project_dataset_table = DESTINATION_BQ_TABLE, 


                    schema_fields = [
                        
                        {"name": "Country or Area", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "Year", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "co2_gigagrams", "type": "FLOAT", "mode": "NULLABLE"}, 
                        {"name": "hfc_gigagrams", "type": "FLOAT", "mode" : "NULLABLE" }, 
                        {"name": "methane_gigagrams", "type": "FLOAT", "mode": "NULLABLE" }, 
                        {"name": "pfc_gigagrams", "type" : "FLOAT", "mode": "NULLABLE" },
                        {"name": "sf6_gigagrams", "type": "FLOAT", "mode": "NULLABLE" }, 
                        {"name": "n2o_gigagrams", "type": "FLOAT", "mode": "NULLABLE"},
                        ], 
                    source_format = "CSV", 
                    skip_leading_rows = 1, # Ignorer l'entete 
                    write_disposition = 'WRITE_TRUNCATE', 
                    create_disposition = 'CREATE_IF_NEEDED', 
                    gcp_conn_id='google_cloud_default'

        
    



    ) 

    # 2. Créatio  d'une vue dans BigQuery 

    create_aggr_bg_table = BigQueryInsertJobOperator(
                    task_id = 'create_bg_view', 
                   configuration = {

                            "query": {
                                "query": """CREATE OR REPLACE VIEW `yellow-taxi-trips-analytic2.GreenHouseDataset.table_view`
                                AS 
                                SELECT 
                                    Year,
                                    SUM (co2_gigagrams) AS total_co2_gigagrams, 
                                FROM 
                                    `yellow-taxi-trips-analytic2.GreenHouseDataset.greenhouse`
                                GROUP BY
                                 Year""", 
                                "useLegacySql": False
                                
                                
                        
                        
                }
                
                },
                gcp_conn_id = "google_cloud_default"
    ) 

    # Dummy and task 
    end = DummyOperator(task_id = "end", 
                        dag = dag, )


    # tasks dependency 
    ## Orchestration dans BigQuery 

    start >> load_csv_to_bq >> create_aggr_bg_table >> end 