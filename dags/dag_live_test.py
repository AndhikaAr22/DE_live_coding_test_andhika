from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from dags.connector.connection_test import ConnectionDB
from airflow.models import Connection

def get_data_gsheet():

    document_id = Variable.get('document_id_gsheet')
    tab_name = "Sheet1"
    full_url = f"https://docs.google.com/spreadsheets/d/{document_id}/gviz/tq?tqx=out:csv&sheet={tab_name}"

    df = pd.read_csv(full_url)
    df = df.iloc[:, :12]
    df['pop'] = df['pop'].fillna('0')
    df['pop'] = df['pop'].str.replace('.', '').astype(int)
    print(df.head())
    return df

def load_data_postgres():
    postgres_conn = Connection.get_connection_from_secrets('conn_postgre_test')
    conn = ConnectionDB(
        postgres_host=postgres_conn.host,
        postgres_db=postgres_conn.schema,
        postgres_user=postgres_conn.login,
        postgres_password=postgres_conn.password,
        postgres_port=postgres_conn.port
    )
    data = get_data_gsheet()
    aunt_postgre = conn.postgres_connection()
    data.to_sql(name='energyinst_panel', con=aunt_postgre, if_exists='replace',index=False)
    print("succes insert to db")




def task_load_data():
    load_data_postgres()

# Definisikan DAG
default_args = {
    'owner': 'andhika',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='load_data_to_postgresql',
    default_args=default_args,
    description='DAG ini bertujuan untuk mengambil data dari Google Sheets dan memuatnya ke PostgreSQL',
    schedule_interval=None,
    catchup=False
) as dag:

    start_task = DummyOperator(
        task_id='start_task',
    )

    task1 = PythonOperator(
        task_id='load_data_postgres',
        python_callable=task_load_data,
        provide_context=True,
    )

    end_task = DummyOperator(
        task_id='end_task',
    )

    start_task >> task1 >> end_task
