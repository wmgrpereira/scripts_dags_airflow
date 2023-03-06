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
PROCESSING = getenv("PROCESSING","airflowprocess" if AMBIENTE == 'PROD' else 'airflowprocess-dev')

clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)

zone_landing = 'airflowlanding-dev'
zone_processing = 'airflowprocess-dev'
past = 'crm/carga_t_fatura/'
n_arquivo = 't_clientes_vuon.parquet'
c5_dev = 'c5_dev'


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
        clientMinio.remove_object(LANDING, (past + n_arquivo))
        clientMinio.fput_object(LANDING, (past + n_arquivo), n_arquivo)

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
    #schedule_interval= "05 13 * * *",
    #schedule_interval="@daily",
    #schedule_interval=timedelta(hours=24),
    default_view="graph",
    catchup=False,
    default_args = default_args,
    tags=["crm", "dados_clientes_vuon"], # If set, this tag is shown in the DAG view of the Airflow UI
)

def crm_carga_t_fatura():

    init = DummyOperator(task_id="Init")
    t_fatura_1= ConsultasHive(task_id = "t_fatura_1", tb = "t_fatura_1")
    t_fatura_2= ConsultasHive(task_id = "t_fatura_2", tb = "t_fatura_2")
    t_fatura_3= ConsultasHive(task_id = "t_fatura_3", tb = "t_fatura_3")
    t_fatura_4= ConsultasHive(task_id = "t_fatura_4", tb = "t_fatura_4")
    t_fatura_5= ConsultasHive(task_id = "t_fatura_5", tb = "t_fatura_5")
    t_fatura_6= ConsultasHive(task_id = "t_fatura_6", tb = "t_fatura_6")
    t_fatura_7= ConsultasHive(task_id = "t_fatura_7", tb = "t_fatura_7")
    t_fatura_8= ConsultasHive(task_id = "t_fatura_8", tb = "t_fatura_8")
    t_fatura_9= ConsultasHive(task_id = "t_fatura_9", tb = "t_fatura_9")
    t_fatura_10= ConsultasHive(task_id = "t_fatura_10", tb = "t_fatura_10")
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    #chain(init, Extrating_Hive, Processing_df, Insere_Oracle, finish)
    chain (init, t_fatura_1, t_fatura_2, t_fatura_3, t_fatura_4, t_fatura_5,
                 t_fatura_6, t_fatura_7, t_fatura_8, t_fatura_9, t_fatura_10, finish)

#Instanciando a dag:
dag = crm_carga_t_fatura()

def config_task(name_task):
    tasks = {
        "t_fatura_1":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '0' and '2176918'
                        """
        },
        "t_fatura_2":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '2176917' and '4176917'
                        """
        },
        "t_fatura_3":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '4176918' and '6176917'
                        """
        },
        "t_fatura_4":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '6176918' and '8176917'
                        """
        },
        "t_fatura_5":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '8176918' and '10176917'
                        """
        },
        "t_fatura_6":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '10176918' and '12176917'
                        """
        },
        "t_fatura_7":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '12176918' and '14176917'
                        """
        },
        "t_fatura_8":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '14176918' and '16176917'
                        """
        },
        "t_fatura_9":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura BETWEEN '16176918' and '18176917'
                        """
        },
        "t_fatura_10":{
            "sql": """SELECT tf.id_fatura                      as  id_fatura,
                            tf.id_processamento              as  id_processamento,
                            tf.id_conta                      as  id_conta,
                            tf.id_ciclo                      as  id_ciclo,
                            tf.id_fatura_anterior            as  id_fatura_anterior,
                            tf.dt_processamento              as  dt_processamento,
                            tf.dt_vencimento                 as  dt_vencimento,
                            tf.fl_ativa                      as  fl_ativa,
                            tf.fl_envia_email                as  fl_envia_email,
                            tf.fl_realiza_impressao          as  fl_realiza_impressao,
                            tf.fl_envia_sms_directone        as  fl_envia_sms_directone,
                            tf.vl_fatura                     as  vl_fatura,
                            tf.vl_pagamento_minimo           as  vl_pagamento_minimo,
                            tf.vl_total_pago                 as  vl_total_pago,
                            tf.dt_inclusao                   as  dt_inclusao
                    FROM vuon_db1.t_Fatura tf
                    WHERE tf.id_fatura >= '18176918'
                        """
        }    
    }
    return tasks[name_task]