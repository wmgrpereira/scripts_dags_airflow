from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.operators.oracle_operator import OracleOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import BaseOperator
#from minio.commonconfig import REPLACE, CopySource
from airflow.models import Variable
from minio import Minio
from os import getenv
#from io import BytesIO
import pandas as pd
from io import BytesIO
from airflow.providers.oracle.hooks.oracle import OracleHook


#["Variáveis AIRFLOW"]
MINIO = Variable.get('MINIO_URL')
ACESS_KEY = Variable.get('MINIO_ACESS_KEY')
SECRET_ACCESS = Variable.get('MINIO_SECRET_ACCESS')
MINIO_REGION = Variable.get('MINIO_REGION')

AMBIENTE = Variable.get('AMBIENTE')

CURATED = getenv("CURATED","airflowcurated" if AMBIENTE == 'PROD' else 'airflowcurated-dev')
LANDING = getenv("LANDING","airflowlanding" if AMBIENTE == 'PROD' else 'airflowlanding-dev')
DATA_LAKE_VUON = getenv("DATA_LAKE_VUON","data-lake-vuon" if AMBIENTE == 'PROD' else "data-lake-vuon-dev" )
PROCESSING = getenv("PROCESSING","airflowprocess" if AMBIENTE == 'PROD' else 'airflowprocess-dev')

clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)

bucket = 'data-lake-vuon'
past = 'hive/'
n_arquivo = 't_venda.parquet'


class HiveUtils():

    def __init__(self, conn_id, schema, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.schema = schema

    def readDataHeader(self, sql):
        print("- HiveUtils -")
        hh = HiveServer2Hook(hiveserver2_conn_id=self.conn_id)
        #rs = hh.get_records(sql, schema=self.schema)
        aux = hh.get_results(sql, schema=self.schema)
        data =aux['data']
        header = [c[0] for c in aux['header']]
        return data, header
    
    def insereHeader(self, df, header):
        self.df = df
        self.header = header

        colunas_atuais = [item for item in df.columns]
        colunas_alteradas = header
        dict = {}
        for i in range(len(colunas_atuais)):
            dict[colunas_atuais[i]] = colunas_alteradas[i]
        df = df.rename(columns=dict)
        return df

class ConsultasHive(BaseOperator):
    def __init__(self, tb = str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tb = tb
    
    def execute(self, context):
     
        config = config_task (self.tb)#("step_stg_venda")
        sql = config["sql"]
        n_arquivo = (f'{self.tb}.parquet')
        
        hive_utils = HiveUtils(conn_id="HIVE_BIGDATA", schema="vuon_db1")
        rs, header = hive_utils.readDataHeader(sql)
        #print(MINIO,ACESS_KEY,SECRET_ACCESS)
        #print(header)
        df = pd.DataFrame(rs)#, columns = ['Name', 'Age'])
        #rs.head()
        df.columns = df.columns.astype(str) #Transforma colunas em string
        df = hive_utils.insereHeader(df, header) #Insere Cabeçalho
        print('TOTAL DE LINHAS: ', len(df))
        #print(df.head())
        
        df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        clientMinio.remove_object(DATA_LAKE_VUON, (past + n_arquivo))
        clientMinio.fput_object(DATA_LAKE_VUON, (past + n_arquivo), n_arquivo)

#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":"wanderley.goncalves@grpereira.com.br",
        #"email_on_failure": False,
        #"email_on_retries": 1,
        "retries": 3,  # If a task fails, it will retry 1 times
        "retry_delay": timedelta(minutes=1) #rodar novamente em caso de falha
    }

@dag(
    start_date=datetime(2022, 11, 1),
    max_active_runs=1, #total de execuções simultâneas
    schedule_interval= "05 13 * * *",
    #schedule_interval="@daily",
    #schedule_interval=timedelta(hours=24),
    default_view="graph",
    catchup=False,
    default_args = default_args,
    tags=["crm", "carga_t_venda"], # If set, this tag is shown in the DAG view of the Airflow UI
)

def crm_carga_t_venda():

    init = DummyOperator(task_id="Init")
    t_venda= ConsultasHive(task_id = "t_venda", tb = "t_venda")
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    #chain(init, Extrating_Hive, Processing_df, Insere_Oracle, finish)
    chain (init, t_venda, finish)

#Instanciando a dag:
dag = crm_carga_t_venda()

def config_task(name_task):
    tasks = {
        "t_venda":{
            "sql": """SELECT tv.id_conta as id_conta,
                            max(tv.dt_venda) as dt_venda_max,
                            min(tv.dt_venda) as dt_venda_min
                        FROM vuon_db1.t_venda tv
                        WHERE tv.fl_status = 'A'
                        GROUP BY tv.id_conta"""
        }
    }
    return tasks[name_task]