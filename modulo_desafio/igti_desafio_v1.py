from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, task
from datetime import datetime

import boto3
import pandas as pd
import pymongo
import requests
import sqlalchemy

path = '/usr/local/airflow/data/'

now = datetime.now()

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date': datetime(now.year, now.month, now.day),
    'email': ['nilson.cunhan@gmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
}

# Definindo a DAG - Fluxo
dag = DAG(
    'desafio_igti', # Nome da DAG
    description='Desafio final do curso Engenheiro de Dados - IGTI', # Descrição da DAG
    default_args=default_args,
    schedule_interval=None # qual intervalo vamos executar a DAG
)

# =====================================================================================
#                                         Dados do IBGE
# =====================================================================================

# Obter os dados através da API do IBGE
def tratar_dados_ibge(dados):
    '''
    Essa função retorna irá retornar uma lista por linha. A variável que receberá os dados dessa função
    terá várias listas sendo cada uma delas uma linha da api
    :param dados: json do ibge
    :return: lista de dados
    '''
    df = pd.DataFrame(dados).reset_index(drop=True).iloc[:1]
    # Removendo as colunas que o pandas cria ao ler o Json
    df.drop(columns=['id', 'nome', 'municipio'], inplace=True)
    df['distrito_id'] = dados['id']
    df['distrito'] = dados['nome']
    df['municipio_id'] = dados['municipio']['id']
    df['municipio'] = dados['municipio']['nome']
    df['microrregiao_id'] = dados['municipio']['microrregiao']['id']
    df['microrregiao'] = dados['municipio']['microrregiao']['nome']
    df['mesorregiao_id'] = dados['municipio']['microrregiao']['mesorregiao']['id']
    df['mesorregiao'] = dados['municipio']['microrregiao']['mesorregiao']['nome']
    df['uf_id'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['id']
    df['uf_sigla'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['sigla']
    df['uf'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['nome']
    df['regiao_id'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['id']
    df['regiao_sigla'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['sigla']
    df['regiao'] = dados['municipio']['microrregiao']['mesorregiao']['UF']['regiao']['nome']

    return df

def obter_dados_ibge():
    '''
    Essa função envia o Json para "tratar_dados_ibge" que retornará uma lista com cada linha.
    Logo em seguida a função obter_dados_ibge irá concatenar essas listas gerando um dataframe final
    :return: dataframe
    '''
    url = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')
    dados = url.json()
    df_preparar = [tratar_dados_ibge(dados[x]) for x in range(len(dados))]
    df_tratado = pd.concat(df_preparar, ignore_index=True)
    df_tratado.to_csv(path+'ibge.csv', index=False, sep=',')
    print(df_tratado.shape)

task_obter_dados_ibge = PythonOperator(
    task_id='obter_dados_ibge',
    python_callable=obter_dados_ibge,
    dag=dag
)

def upload_dados_ibge():
    access_key = Variable.get('aws_access_key_id')
    secret_key = Variable.get('aws_secret_access_key')
    aws_s3_nome = Variable.get('aws_s3_nome')
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.upload_file(path+'ibge.csv', aws_s3_nome, "ibge.csv")

task_upload_dados_ibge = PythonOperator(
    task_id='upload_dados_ibge',
    python_callable=upload_dados_ibge,
    dag=dag
)

def ingestao_dw_ibge():
    aws_mysql_secret = Variable.get('aws_secret_access_mysql')
    aws_server_mysql = Variable.get('aws_server_mysql')
    engine = sqlalchemy.create_engine(f'mysql://admin:{aws_mysql_secret}@{aws_server_mysql}/dwigti')
    ibge = pd.read_csv(path+'ibge.csv')
    ibge.to_sql('tb_ibge', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)

task_ingestao_dw_ibge = PythonOperator(
    task_id='ingestao_dw_ibge',
    python_callable=ingestao_dw_ibge,
    dag=dag
)

# =====================================================================================
#                                  Dados do API MongoDB
# =====================================================================================

def obter_dados_api():
    senha_igti = Variable.get('secret_igti_mongo')
    usuario_igti = Variable.get('usuario_igti_mongo')
    client = pymongo.MongoClient(f"mongodb+srv://{usuario_igti}:{senha_igti}@unicluster.ixhvw.mongodb.net/")
    db = client.ibge
    query = db.pnadc20203.find({}, {'_id': 0}) # Retorna todos os dados da API

    df = pd.DataFrame(list(query))
    df.to_csv(path+'api.csv', index=False, sep=',')
    print(df.shape)

task_obter_dados_api = PythonOperator(
    task_id='obter_dados_api',
    python_callable=obter_dados_api,
    dag=dag
)

def upload_dados_api():
    access_key = Variable.get('aws_access_key_id')
    secret_key = Variable.get('aws_secret_access_key')
    aws_s3_nome = Variable.get('aws_s3_nome')
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3.upload_file(path+'api.csv', aws_s3_nome, "api.csv")

task_upload_dados_api = PythonOperator(
    task_id='upload_dados_api',
    python_callable=upload_dados_api,
    dag=dag
)

def ingestao_dw_api():
    aws_mysql_secret = Variable.get('aws_secret_access_mysql')
    aws_server_mysql = Variable.get('aws_server_mysql')
    engine = sqlalchemy.create_engine(f'mysql://admin:{aws_mysql_secret}@{aws_server_mysql}/dwigti')
    api = pd.read_csv(path+'api.csv')
    api_dw = api.loc[(api.idade >= 20) & (api.idade <= 40) & (api.sexo == 'Mulher')]
    api_dw.to_sql('tb_api', con=engine, index=False, if_exists='replace', method='multi', chunksize=1000)

task_ingestao_dw_api = PythonOperator(
    task_id='ingestao_dw_api',
    python_callable=ingestao_dw_api,
    dag=dag
)

task_obter_dados_ibge >> [task_upload_dados_ibge, task_ingestao_dw_ibge]
task_obter_dados_api >> [task_upload_dados_api, task_ingestao_dw_api]
