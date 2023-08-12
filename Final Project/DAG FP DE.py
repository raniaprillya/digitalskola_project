from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import importlib

# Step 1: Define DAG
default_args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id = 'FP_DE_Rani',
    default_args=default_args,
    description='My Python DAG',
    schedule_interval="@daily",
    start_date=datetime(2023, 7, 1),
    catchup=False
)

# Step 2: Import Python Module
module_name = 'connect_db'
module_name2 = 'main'
module_name3 = 'transform'
module_name4 = 'graph'
my_module = importlib.import_module(module_name)
my_module2 = importlib.import_module(module_name2)
my_module3 = importlib.import_module(module_name3)
my_module4 = importlib.import_module(module_name4)

# Step 3: DatabaseConnection Class instance required arguments
data_processor = my_module.DatabaseConnection(
    host="localhost",
    database="fp_rn_raw",
    user="rani",
    password="123456",
)

# Step 4: Define the tasks using PythonOperator and pass the callable objects (methods)

data_processing_task_1 = PythonOperator(
    task_id='test_connection',
    python_callable=data_processor.connect,
    dag=dag,
)

# Repeating Step 3 & 4

data_processor2 = my_module2.DataProcessor(
    api_url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
)

data_processing_task_3 = PythonOperator(
    task_id='data_processing',
    python_callable=data_processor2.main,
    dag=dag,
)

data_processor3 = my_module3.transformProcessor()

data_processing_task_4 = PythonOperator(
    task_id='data_transformation',
    python_callable=data_processor3.main_trans,
    dag=dag,
)

db_params = {
            'host': 'localhost',
            'database': 'fp_rn_result',
            'user': 'rani',
            'password': '123456',
            'port': '5432'
        }
data_processor4 = my_module4.GraphFromPostgreSQL(db_params)

data_processing_task_5 = PythonOperator(
    task_id='graph_create',
    python_callable=data_processor4.main_graph,
    dag=dag,
)

# Step 5: Set task dependencies
data_processing_task_1 >> data_processing_task_3 >> data_processing_task_4 >> data_processing_task_5

# Step 6: Instantiate the DAG
dag
