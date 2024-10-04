import datetime
import os
import json
import random

from airflow import models
from airflow import DAG
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.operators import dataproc_operator
from airflow.providers.google.cloud.operators import dataproc
from airflow.utils import trigger_rule
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.contrib.operators.dataproc_operator import DataProcSparkOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

yesterday = datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(1),
                                   datetime.datetime.min.time())

default_dag_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_success': False, 
    #'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('Project_ID')
}

dag_config = models.Variable.get("MailSender", deserialize_json=True)

client_id = dag_config["ClientID"]
client_secret = dag_config["ClientSecret"]
tenant_id = dag_config["TenantID"]
user_email = dag_config["UserEmail"]
#recipient_email = dag_config["RecipientEmail"]

subject = dag_config["FMCSubject"]
message_content = "fmc"


def ensure_cluster_exists(**kwargs):
    cluster = None
    try:
        cluster = DataProcHook(gcp_conn_id='Prod_Comoser_load').get_conn().projects().regions().clusters().get(
            projectId=models.Variable.get('Project_ID'),
            region=models.Variable.get('Region'),
            clusterName=models.Variable.get('Cluster_Name2')
        ).execute(num_retries=5)
        print("CLUSTER is EXIST")
    except Exception as ex:
        print("CLUSTER NOT EXIST")
        print(ex)
        
    if cluster is None or len(cluster) == 0 or 'clusterName' not in cluster:
        return 'create_dataproc_cluster-sasload-Prod'
    else:
        return 'cluster_exist'

dag = DAG(
        'MailSender_FMC_BIMC_DAG',
        schedule_interval='@monthly',
        default_args=default_dag_args)
    
    
def send_email_function(**kwargs):

    from MailSenderWithArgs import send_email

    # Call the send_email function with required arguments
    send_email(
        client_id,
        client_secret,
        tenant_id,
        user_email,       
        subject,
        message_content
    )

# Define the PythonOperator
send_email_task = PythonOperator(
    task_id='send_email_task',
    python_callable=send_email_function,
    provide_context=True,  # This allows passing the context (kwargs) to the function
    dag=dag,
)

# Set the task dependencies
send_email_task

if __name__ == "__main__":
    dag.cli()