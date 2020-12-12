# Dag Schedulada para desafio

from airflow import DAG
# executar comandos Linux e Python
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import pandas as pd
import zipfile
import sqlalchemy
import json

# Constantes
data_path = '/home/nilson/Documentos/igti_engenheiro_de_dados/modulo1/desafio/'
arquivo = data_path + 'collected_tweets_2020-12-08-09-54-42.txt'

# Argumentos default
default_args = {
    'owner': 'Nilson Cunha',
    'depends_on_past': False, # definir se depende de alguma coisa
    'start_date' : datetime(2020, 12, 8, 10, 30),
    'email': ['nilson.cunhan@gmail.com', 'nilsonjneto@hotmail.com'], # ativando e-mail
    'email_on_failure': False, # se der falha True envia e-mail
    'email_on_retry': False, # tenta enviar novamente,
    #'retries': 1, # quantidade de tentativas
    #'retry_delay': timedelta(minutes=2)
}

# Definindo a DAG - Fluxo
dag = DAG(
    'treino-06', # Nome da DAG
    description='Desafio - Tratando dados do tweeter', # Descrição da DAG
    default_args=default_args, # Definido anteriormente
    # podemos colocar no schedule 3 maneiras
    # 1 - quando não queremos fazer o schedule colocamos '@once' ou None ou outra(verificar)
    # 2 - utilizando cron. Abaixo uso para fazer o schedule a cada 2 minutos
    schedule_interval=None # qual intervalo vamos executar a DAG
)

def tweet_para_df(tweet):
    try:
        df_tratado = pd.DataFrame(tweet).reset_index(drop=True).iloc[:1]

        # Dropando as colunas que não utilizaremos
        df_tratado.drop(columns=['quote_count', 'reply_count', 'retweet_count', 'favorite_count',
                                 'favorited', 'retweeted', 'user', 'entities', 'retweeted_status'],
                        inplace=True)

        # Criando novas colunas
        df_tratado['user_id'] = tweet['user']['id']
        df_tratado['user_id_str'] = tweet['user']['id_str']
        df_tratado['user_screen_name'] = tweet['user']['screen_name']
        df_tratado['user_location'] = tweet['user']['location']
        df_tratado['user_description'] = tweet['user']['description']
        df_tratado['user_protected'] = tweet['user']['protected']
        df_tratado['user_verified'] = tweet['user']['verified']
        df_tratado['user_followers_count'] = tweet['user']['followers_count']
        df_tratado['user_friends_count'] = tweet['user']['friends_count']
        df_tratado['user_created_at'] = tweet['user']['created_at']

        # Buscando as mentions
        user_mentions = []
        for i in range(len(tweet['entities']['user_mentions'])):
            dicionario_base = tweet['entities']['user_mentions'][i].copy()
            dicionario_base.pop('indices', None)
            df = pd.DataFrame(dicionario_base, index=[0])
            df = df.rename(columns={
                'screen_name': 'entities_screen_name',
                'name': 'entities_name',
                'id': 'entities_id',
                'id_str': 'entities_id_str'
            })
            user_mentions.append(df)

        # concatenando as mentions para concatenar no dataframe
        dfs = []
        for i in user_mentions:
            dfs.append(
                pd.concat([df_tratado.copy(), i], axis=1)
            )

        # concatenando os dados
        df_final = pd.concat(dfs, ignore_index=True)

    except:
        return None

    return df_final

def ler_arquivo():
    # lendo o arquivo
    with open(arquivo, 'r') as file:
        tweets = file.readlines()

    parsed_tweets = [json.loads(json.loads(i)) for i in tweets]

    parseados = [tweet_para_df(tweet) for tweet in parsed_tweets]
    parseados = [i for i in parseados if i is not None]

    tratado = pd.concat(parseados, ignore_index=True)

    tratado.to_csv(data_path+'desafio.csv', index=False)

task_arquivo = PythonOperator(
    task_id='arquivo',
    python_callable=ler_arquivo,
    dag=dag
)

def escreve_dw():
    df = pd.read_csv(data_path+'desafio.csv')
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://root:root@localhost/desafio'
    )
    df.to_sql('tratado', con=engine, index=False, if_exists='append')

task_escreve_dw = PythonOperator(
    task_id='escreve_dw',
    python_callable=escreve_dw,
    dag=dag
)

task_arquivo >> task_escreve_dw