# Dag Schedulada para dados do Titanic

from airflow import DAG
# executar comandos Linux e Python
from airflow.operators.bash_operator import BaseOperator, BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from datetime import datetime, timedelta
import pandas as pd
import random

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date': datetime(2020, 12, 3, 9, 40),
    'email': ['nilson.cunhan@gmail.com', 'nilsonjneto@hotmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
    'retries': 1, # quantidade de tentativas
    'retry_delay': timedelta(minutes=2)
}

# Definindo a DAG - Fluxo
dag = DAG(
    'treino-03', # Nome da DAG
    description='Extrair dados do Titanic da internet e calcular a idade média de homem ou mulher', # Descrição da DAG
    default_args=default_args, # Definido anteriormente
    # podemos colocar no schedule 3 maneiras
    # 1 - quando não queremos fazer o schedule colocamos '@once' ou None ou outra(verificar)
    # 2 - utilizando cron. Abaixo uso para fazer o schedule a cada 2 minutos
    schedule_interval="*/2 * * * *" # qual intervalo vamos executar a DAG
)

get_data = BashOperator(
    # nome da task
    task_id='get_data',
    # baixando o arquivo
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv',
    # informando a dag
    dag=dag
)

def sorteia_h_m():
    return random.choice(['male', 'female'])

escolhe_h_m = PythonOperator(
    task_id='escolhe-h-m',
    python_callable=sorteia_h_m,
    dag=dag
)

def MouF(**context):
    value = context['task_instance'].xcom_pull(task_ids='escolhe-h-m')
    if value == 'male':
        return 'branch_homem'
    if value == 'female':
        return 'branch_mulher'

# BranchPythonOperator -> Utilizado para uma condicional
male_female = BranchPythonOperator(
    task_id='condicional',
    python_callable=MouF,
    provide_context=True, # Pegando o valor retornado
    dag=dag
)

def mean_homem():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex == 'male']
    print(f'Média de idade dos homens no Titanic {df.Age.mean()}')

branch_homem = PythonOperator(
    task_id='branch_homem', # nome da task
    python_callable=mean_homem, # chamando a função
    dag=dag
)

def mean_mulher():
    df = pd.read_csv('/usr/local/airflow/data/train.csv')
    df = df.loc[df.Sex == 'female']
    print(f'Média de idade das mulheres no Titanic {df.Age.mean()}')

branch_mulher = PythonOperator(
    task_id='branch_mulher',
    python_callable=mean_mulher,
    dag=dag
)

get_data >> escolhe_h_m >> male_female >> [branch_homem, branch_mulher]

# subindo os containers no Docker com 2 workers
# docker-compose up -d --scale worker==2