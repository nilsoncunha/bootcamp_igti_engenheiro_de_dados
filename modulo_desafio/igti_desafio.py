from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import pandas as pd
import pymongo
import requests

now = datetime.now()

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date': datetime(now.year, now.month, now.day),
    'email': ['nilson.cunhan@gmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
    #'retries': 1, # quantidade de tentativas
    #'retry_delay': timedelta(minutes=1)
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
url = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')

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
    dados = url.json()

    df_preparar = [tratar_dados_ibge(dados[x]) for x in range(len(dados))]
    df_tratado = pd.concat(df_preparar, ignore_index=True)

    print(df_tratado.shape)

task_obter_dados_ibge = PythonOperator(
    task_id='obter_dados_ibge',
    python_callable=obter_dados_ibge,
    dag=dag
)

# =====================================================================================
#                                  Dados do API MongoDB
# =====================================================================================

def tratar_dados_api(dados):
    '''
    Essa função irá gerar uma lista para cada linha do json e retornará um conjunto de listas
    :param dados: json API
    :return: lista de dados
    '''
    # Passando os dados como lista, caso contrário ocorrerá erro.
    df = pd.DataFrame([dados]).reset_index(drop=True).iloc[:1]

    return df

def obter_dados_api():
    client = pymongo.MongoClient("mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net/")
    db = client.ibge
    query = db.pnadc20203.find({}, {'_id': 0}) # Retorna todos os dados da API

    dados_api = []
    for x in query:
        dados_api.append(x)

    df_preparar_api = [tratar_dados_api(dados_api[x]) for x in range(len(dados_api))]
    df_tratado_api = pd.concat(df_preparar_api, ignore_index=True)

    print(df_tratado_api.shape)



