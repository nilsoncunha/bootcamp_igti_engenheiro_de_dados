from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

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

@dag(default_args=default_args, schedule_interval=None, description='Desafio final do curso Engenheiro de Dados - IGTI - Airflow v2.0')
def desafio_final_pipeline():
    '''
    Pipeline no qual será extraido os dados da API do IBGE e também de uma API baseada no MongoDB.
    Vou inserir no S3 e fazer a ingestão no Postgresql, simulando um DW.
    '''
    @task
    def dados_ibge():
        import pandas as pd
        import requests

        url = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/distritos')
        dados = url.json()

        df = pd.DataFrame(
                 dados, 
                 columns=['id','nome','municipio','microrregiao','mesorregiao','uf_sigla','uf','regiao_sigla','regiao'],
                 )
        df = df.rename(columns={'id':'id_distrito', 'nome':'distrito'})
    
        for x in range(len(df)):
            df['microrregiao'][x] = df['municipio'][x]['microrregiao']['nome']
            df['mesorregiao'][x] = df['municipio'][x]['microrregiao']['mesorregiao']['nome']
            df['uf_sigla'][x] = df['municipio'][x]['microrregiao']['mesorregiao']['UF']['sigla']
            df['uf'][x] = df['municipio'][x]['microrregiao']['mesorregiao']['UF']['nome']
            df['regiao_sigla'][x] = df['municipio'][x]['microrregiao']['mesorregiao']['UF']['regiao']['sigla']
            df['regiao'][x] = df['municipio'][x]['microrregiao']['mesorregiao']['UF']['regiao']['nome']
            df['municipio'][x] = df['municipio'][x]['nome'] 
        
        df['id_distrito'] = df['id_distrito'].astype('object')
        df.to_csv(path+'ibge.csv', index=False, sep=';', encoding='utf-8')
        return path+'ibge.csv'

    @task
    def dados_api():
        import pymongo
        import pandas as pd

        usuario_igti = Variable.get('usuario_igti_mongo')
        senha_igti = Variable.get('secret_igti_mongo')
        client = pymongo.MongoClient(f"mongodb+srv://{usuario_igti}:{senha_igti}@unicluster.ixhvw.mongodb.net/")
        db = client.ibge
        colecao = db.pnadc20203.find({}, {'_id':0})
        df = pd.DataFrame(colecao)
        
        col_object = ['graduacao', 'trab', 'ocup']
        col_float = ['renda', 'horastrab', 'anosesco']
        for coluna in col_object:
            df[coluna] = df[coluna].fillna('Não informado')
            
        for coluna in col_float:
            df[coluna] = df[coluna].fillna(0)
        
        df.to_csv(path+'api.csv', index=False, sep=';', encoding='utf-8')
        return path+'api.csv'

    @task
    def upload_s3(caminho_arquivo):
        import boto3

        aws_access_key_id = Variable.get('aws_access_key_id')
        aws_secret_access_key = Variable.get('aws_secret_access_key')
        aws_s3_nome = Variable.get('aws_s3_nome')
        client = boto3.client(
            's3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
        )
        client.upload_file(caminho_arquivo, aws_s3_nome, caminho_arquivo[24:]) # Caminho arquivo, nome_bucket, nome_arquivo

    @task
    def escrever_dw(arquivo_csv):
        import pandas as pd
        from sqlalchemy.engine import create_engine

        aws_usuario_postgres = Variable.get('aws_usuario_postgres')
        aws_senha_postgres = Variable.get('aws_secret_postgres')
        aws_server_postgres = Variable.get('aws_server_postgres')
        conn = create_engine(f"postgresql://{aws_usuario_postgres}:{aws_senha_postgres}@{aws_server_postgres}:5432/igtidesafio")
        df = pd.read_csv(arquivo_csv, sep=';')
        if arquivo_csv == path+'api.csv':
            df = df.loc[(df.idade >= 20) & (df.idade <= 40) & (df.sexo == 'Mulher')]
        df.to_sql(arquivo_csv[24:-4], conn, index=False, if_exists='replace', method='multi', chunksize=1000)

    ibge = dados_ibge()
    api = dados_api()
    s3_ibge = upload_s3(ibge)
    s3_api = upload_s3(api)
    dw_ibge = escrever_dw(ibge)
    dw_api = escrever_dw(api)

desafio_final = desafio_final_pipeline()
