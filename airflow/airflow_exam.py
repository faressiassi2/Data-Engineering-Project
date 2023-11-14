from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess as sp

def extract():
	sp.run(['python3', './extract.py'])
	
def analyse():
	sp.run(['python3', './analyse_traitement.py'])
	
def model():
	sp.run(['python3', './modelisation.py'])
	
def evall():
	sp.run(['python3', './evaluation.py'])
	

with DAG(dag_id="sujet_exam", start_date=datetime(2023,11,14), schedule_interval='@hourly', catchup=False) as dag:
  task1=PythonOperator(task_id="extract",python_callable=extract)
  task2=PythonOperator(task_id="analyse_trait",python_callable=analyse)
  task3=PythonOperator(task_id="modelisation",python_callable=model)
  task4=PythonOperator(task_id="evaluation",python_callable=evall)
  
task1 >> task2 >> task3 >> task4

