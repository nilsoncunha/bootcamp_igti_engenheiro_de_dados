# primeira DAG com Airflow

from airflow import DAG
# executar comandos Linux e Python
from airflow.operators.bash_operator import BaseOperator, BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date': datetime(2020, 12, 1, 22),
    'email': ['nilson.cunhan@gmail.com', 'nilsonjneto@hotmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
    'retries': 1, # quantidade de tentativas
    'retry_delay': timedelta(minutes=1) # tempo para tentar novamente
}

# Definindo a DAG - Fluxo
dag = DAG(
    'treino-01', # Nome da DAG
    description='Básico de Bash Operators e Python Operators', # Descrição da DAG
    default_args=default_args, # Definido anteriormente
    schedule_interval=timedelta(minutes=2) # qual intervalo vamos executar a DAG
)

# Começando a adicionar tarefas
# No BashOperator definimos o comando para executar em 'bash_comand'
hello_bash = BashOperator(
    task_id='Hello_Bash', # qual ID dessa task no fluxo
    bash_command='echo "Hello Airflow from bash"',
    dag=dag
)

# No PythonOperator definimos uma função que executará o comando
# cria a função e passa ela no parâmetro
def say_hello():
    print('Hello Airflow from Python')

hello_python = PythonOperator(
    task_id='Hello_Python',
    python_callable=say_hello, # função python que será executada
    dag=dag
)

# ordenando qual ordem irá ser executado
hello_bash >> hello_python

# subindo os containers no Docker com 2 workers
# docker-compose up -d --scale worker==2