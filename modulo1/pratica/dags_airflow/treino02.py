# Dag Schedulada para dados do Titanic

from airflow import DAG
# executar comandos Linux e Python
from airflow.operators.bash_operator import BaseOperator, BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date': datetime(2020, 12, 1, 22),
    'email': ['nilson.cunhan@gmail.com', 'nilsonjneto@hotmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
    'retries': 1, # quantidade de tentativas
    'retry_delay': timedelta(minutes=2)
}

# Definindo a DAG - Fluxo
dag = DAG(
    'treino-02', # Nome da DAG
    description='Extrair dados do Titanic da internet e calcular a idade média', # Descrição da DAG
    default_args=default_args, # Definido anteriormente
    # podemos colocar no schedule 3 maneiras
    # 1 - quando não queremos fazer o schedule colocamos '@once' ou None ou outra(verificar)
    # 2 - utilizando cron. Abaixo uso para fazer o schedule a cada 2 minutos
    schedule_interval="*/3 * * * *" # qual intervalo vamos executar a DAG
)

get_data = BashOperator(
    # nome da task
    task_id='get_data',
    # baixando o arquivo
    bash_command='curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv',
    # informando a dag
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('~/train.csv')
    med = df.Age.mean()
    return med

task_idade_media = PythonOperator(
    task_id='calcula-idade-media', # nome da task
    python_callable=calculate_mean_age, # chamando a função
    dag=dag
)

def print_age(**context):
    # pegando as informações da função de calcular média
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f'Idade média no titanic era {value} anos')

task_print_idade = PythonOperator(
    task_id='mostra-idade', # nome da task
    python_callable=print_age, # chamando a função
    provide_context=True, # tem acesso ao context para pegar o valor
    dag=dag
)

get_data >> task_idade_media >> task_print_idade

# subindo os containers no Docker com 2 workers
# docker-compose up -d --scale worker==2