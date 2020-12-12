# Dag Schedulada para dados do Titanic

from airflow import DAG
# executar comandos Linux e Python
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import pandas as pd
import zipfile
import sqlalchemy
import pymysql

# Constantes
data_path = '/home/nilson/airflow/data/microdados_enade_2019/2019/3.DADOS/'
arquivo = data_path + 'microdados_enade_2019.txt'

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
    'treino-05', # Nome da DAG
    description='Paralelismo e intregações para entrega', # Descrição da DAG
    default_args=default_args, # Definido anteriormente
    # podemos colocar no schedule 3 maneiras
    # 1 - quando não queremos fazer o schedule colocamos '@once' ou None ou outra(verificar)
    # 2 - utilizando cron. Abaixo uso para fazer o schedule a cada 2 minutos
    schedule_interval=None # qual intervalo vamos executar a DAG
)

get_data = BashOperator(
    task_id='get_data',
    bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o /home/nilson/airflow/data/microdados_enade_2019.zip',
    dag=dag
)

def unzip_file():
    with zipfile.ZipFile('/home/nilson/airflow/data/microdados_enade_2019.zip', 'r') as zipped:
        zipped.extractall('/home/nilson/airflow/data')

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_file,
    dag=dag
)

def aplica_filtros():
    cols = ['CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 'NT_FG', 'NT_CE',
            'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    enade = pd.read_csv(arquivo, sep=';', decimal=',', usecols=cols)
    enade = enade.loc[
        (enade.NU_IDADE > 20) &
        (enade.NU_IDADE < 40) &
        (enade.NT_GER > 0)
    ]
    enade.to_csv(data_path+'enade_filtrado.csv', index=False)

task_aplica_filtro = PythonOperator(
    task_id='aplica_filtro',
    python_callable=aplica_filtros,
    dag=dag
)

# Idade centralizada na média
# Idade centralizada ao quadrado
# primeiro média depois quadrado

def constroi_idade_centralizada():
    idade = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['NU_IDADE'])
    idade['idadecent'] = idade.NU_IDADE - idade.NU_IDADE.mean()
    idade[['idadecent']].to_csv(data_path+'idadecent.csv', index=False)

task_idade_cent = PythonOperator(
    task_id='constroi_idade_centralizada',
    python_callable=constroi_idade_centralizada,
    dag=dag
)

def constroi_idade_cent_quad():
    idadecent = pd.read_csv(data_path+'idadecent.csv')
    idadecent['idade2'] = idadecent.idadecent ** 2
    idadecent[['idade2']].to_csv(data_path+'idadequadrado.csv', index=False)

task_idade_quad = PythonOperator(
    task_id='constroi_idade_ao_quadrado',
    python_callable=constroi_idade_cent_quad,
    dag=dag
)

def constroi_est_civil():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['QE_I01'])
    filtro['estcivil'] = filtro.QE_I01.replace({
        'A': 'Solteiro',
        'B': 'Casado',
        'C': 'Separado',
        'D': 'Viúvo',
        'E': 'Outro'
    })
    filtro[['estcivil']].to_csv(data_path+'estcivil.csv', index=False)

task_est_civil = PythonOperator(
    task_id='constroi_est_civil',
    python_callable=constroi_est_civil,
    dag=dag
)

def constroi_cor():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['QE_I02'])
    filtro['cor'] = filtro.QE_I02.replace({
        'A': 'Branca',
        'B': 'Preta',
        'C': 'Amarela',
        'D': 'Parda',
        'E': 'Indígena',
        'F': '',
        ' ': ''
    })
    filtro[['cor']].to_csv(data_path+'cor.csv', index=False)

task_cor = PythonOperator(
    task_id='constroi_cor_da_pele',
    python_callable=constroi_cor,
    dag=dag
)

def constroi_escopai():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['QE_I04'])
    filtro['escopai'] = filtro.QE_I04.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filtro[['escopai']].to_csv(data_path+'escopai.csv', index=False)

task_escopai = PythonOperator(
    task_id='constroi_escopai',
    python_callable=constroi_escopai,
    dag=dag
)

def constroi_escomae():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['QE_I05'])
    filtro['escomae'] = filtro.QE_I05.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5
    })
    filtro[['escomae']].to_csv(data_path+'escomae.csv', index=False)

task_escomae = PythonOperator(
    task_id='constroi_escomae',
    python_callable=constroi_escomae,
    dag=dag
)

def constroi_renda():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv', usecols=['QE_I08'])
    filtro['renda'] = filtro.QE_I08.replace({
        'A': 0,
        'B': 1,
        'C': 2,
        'D': 3,
        'E': 4,
        'F': 5,
        'G': 6
    })
    filtro[['renda']].to_csv(data_path+'renda.csv', index=False)

task_renda = PythonOperator(
    task_id='constroi_renda',
    python_callable=constroi_renda,
    dag=dag
)

# Task de JOIN
def join_data():
    filtro = pd.read_csv(data_path+'enade_filtrado.csv')
    idadecent = pd.read_csv(data_path+'idadecent.csv')
    idadeaoquadrado = pd.read_csv(data_path+'idadequadrado.csv')
    estcivil = pd.read_csv(data_path+'estcivil.csv')
    cor = pd.read_csv(data_path+'cor.csv')
    escopai = pd.read_csv(data_path+'escopai.csv')
    escomae = pd.read_csv(data_path+'escomae.csv')
    renda = pd.read_csv(data_path+'renda.csv')

    final = pd.concat([
        filtro, idadecent, idadeaoquadrado, estcivil, cor,
        escopai, escomae, renda
    ], axis=1)

    final.to_csv(data_path+'enade_tratado.csv', index=False)
    print(final)

task_join = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag
)

def escreve_dw():
    final = pd.read_csv(data_path+'enade_tratado.csv')
    engine = sqlalchemy.create_engine(
        'mysql+pymysql://root:root@localhost/enade'
    )
    final.to_sql('tratado', con=engine, index=False, if_exists='append')

task_escreve_dw = PythonOperator(
    task_id='escreve_dw',
    python_callable=escreve_dw,
    dag=dag
)

get_data >> unzip_data >> task_aplica_filtro
task_aplica_filtro >> [task_idade_cent, task_est_civil, task_cor,
                       task_escopai, task_escomae, task_renda]
# informando que a minha task_idade_quad tem que vir depois de task_idade_cent

task_idade_quad.set_upstream(task_idade_cent)
# task_idade_cent.downstream_list(task_idade_quad) - FAZ A MESMA COISA ACIMA

task_join.set_upstream([
    task_est_civil, task_cor, task_escopai, task_escomae, task_renda, task_idade_quad
])

# dizendo que a task_escreve_dw executa depois de task_join
task_join.set_downstream(task_escreve_dw)


# subindo os containers no Docker com 2 workers
# docker-compose up -d --scale worker==2