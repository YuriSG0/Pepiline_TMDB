from airflow.decorators import task,dag
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import requests
import pandas as pd
import pendulum

@dag(
    dag_id = 'dag_tmdb_databricks',
    description= 'The dag desenvolep with objective load data tmdb to Databricks Cloud',
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2026,11,8,tz='America/Sao_Paulo')
)

def dag_tmdb():
    with TaskGroup('Authentification') as authetification:
        @task
        def authentication_TMDB():
            url = Variable.get('url-authetication')
            token = Variable.get('token')
            header = {
                'Authorization' : token
            }

            response = requests.get(url,headers=header)

            return response.status_code
        
        auth = authentication_TMDB()

    with TaskGroup('Get-Data-Film') as get_information:
        @task(TriggerRule=ONE_SUCESS)
        def requests_genres():
            url = Variable.get('url-genres')
            token = Variable.get('token')

            header = {
            'Authorization' : token
            }

            response = requests.get(url,headers=header)
            return response.json()
        
        genres_data = requests_genres()
        
        @task
        def request_film():
            url = Variable.get('url-movie')
            token =  Variable.get('token')

            header = {
                'Authorization' : token
            }

            response = requests.get(url,headers=header)
            return response.json()

        film_data = request_film()

    authetification >> get_information

            
                

dag_tmdb()