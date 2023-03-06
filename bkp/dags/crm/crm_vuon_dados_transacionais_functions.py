from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
from airflow.operators.oracle_operator import OracleOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
#from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.models.baseoperator import BaseOperator
#from minio.commonconfig import REPLACE, CopySource
from airflow.models import Variable
from crm_vuon_dados_transacionais_consultas import config_task, tasks
from minio import Minio
from os import getenv
from io import BytesIO
import pandas as pd
import hashlib

#["Variáveis AIRFLOW"]
MINIO = Variable.get('MINIO_URL')
ACESS_KEY = Variable.get('MINIO_ACESS_KEY')
SECRET_ACCESS = Variable.get('MINIO_SECRET_ACCESS')
MINIO_REGION = Variable.get('MINIO_REGION')
clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)
AMBIENTE = Variable.get('AMBIENTE')

CURATED = getenv("CURATED","airflowcurated" if AMBIENTE == 'PROD' else 'airflowcurated-dev')
LANDING = getenv("LANDING","airflowlanding" if AMBIENTE == 'PROD' else 'airflowlanding-dev')
PROCESSING = getenv("PROCESSING","airflowprocess" if AMBIENTE == 'PROD' else 'airflowprocess-dev')
DATA_LAKE_VUON = getenv("DATA_LAKE_VUON","data-lake-vuon" if AMBIENTE == 'PROD' else "data-lake-vuon-dev" )

clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)

past = 'crm/dados_transacionais/'
n_arquivo = ('dados_transacionais.parquet')


class HiveUtils():

    def __init__(self, conn_id, schema, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.schema = schema

    def readData(self, sql):
        print("- HiveUtils -")
        hh = HiveServer2Hook(hiveserver2_conn_id=self.conn_id)
        #rs = hh.get_records(sql, schema=self.schema)
        data = hh.get_results(sql, schema=self.schema)['data']
        #header = hh.get_results(sql, schema=self.schema)['header']
        return data

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
        df.columns = df.columns.astype(str) #Transforma colunas em string

        df = hive_utils.insereHeader(df, header) #Insere Cabeçalho
        #print('TOTAL DE LINHAS: ', len(df))

        df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        #df2 = pd.read_parquet(n_arquivo)
        #print(df2)
        #print('TOTAL DE LINHAS: ', len(df2))
        
        clientMinio.remove_object(DATA_LAKE_VUON, (past + n_arquivo))
        clientMinio.fput_object(DATA_LAKE_VUON, (past + n_arquivo), n_arquivo)

class Processing(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        #self.tb = tb

    def listaMinio (bucket, past):
        objetos = []
        objects = clientMinio.list_objects(bucket,past)
        for obj in objects:
            objetos.append(obj.object_name.replace(past,""))
        #print(dataSets)
        return (objetos)

    def execute(self, context):

        lst = [x + '.parquet' for x in list(tasks.keys())] # LISTA DOS ARQUIVOS GERADOS
        print(f'LIST: {lst}')

        dataSets = Processing.listaMinio(bucket = DATA_LAKE_VUON,past = past) # LISTA DOS ARQUIVOS GERADOS
        print(f'DATASETS: {dataSets}')

        for file in dataSets:
            titulo = file.replace(".parquet", "")
            path = clientMinio.get_object(DATA_LAKE_VUON,past+file)
            ds = pd.read_parquet(BytesIO(path.data))
            df_init = pd.DataFrame(ds)
            globals()[f'df_{titulo}'] = df_init
            print(f'df_{titulo} : {len(df_init)}')

        #função de processamento
        df_parcelamento_fatura = df_t_parcelamento_fatura 
        df_parcelamento_fatura = df_parcelamento_fatura.drop_duplicates(keep="last")
        df_pagamento_fatura = df_t_pagamento_fatura[["id_fatura","id_conta","vlr_pago","dt_pagamento_ult_fatura"]]
        df_pagamento_fatura["vlr_pago"]= df_pagamento_fatura.groupby("id_fatura")["vlr_pago"].transform('sum')#transform(lambda x: x.sum())
        df_pagamento_fatura["dt_pagamento_ult_fatura"]= df_pagamento_fatura.groupby("id_fatura")["dt_pagamento_ult_fatura"].transform('max')#transform(lambda y: y.max())
        df_pagamento_fatura = df_pagamento_fatura[["id_fatura","id_conta","vlr_pago","dt_pagamento_ult_fatura"]]
        df_pagamento_fatura = df_pagamento_fatura.drop_duplicates()
        df = df_t_fatura.merge(df_ult_fat_emitida , left_on=["id_fatura","id_conta"], right_on = ["id_fatura","id_conta"], how = 'left')
        df = df_t_fatura.merge(df_pagamento_fatura, left_on=["id_fatura","id_conta"], right_on = ["id_fatura","id_conta"], how = 'left')
        df = df.drop_duplicates()
        df = df.merge(df_t_conta, left_on="id_conta", right_on="id_conta", how = "inner")
        df = df.merge(df_t_anuidade_produto[["id_produto","vl_faturamento_minimo"]],left_on="id_produto", right_on="id_produto", how = "inner")
        df = df.merge(df_t_cliente, left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df = df.drop_duplicates()
        df = df.drop(["id_fatura"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_venda, left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df['dt_cadastro'] = pd.to_datetime(df['dt_cadastro']).dt.date #dt.date
        df['dt_ultima_compra'] = pd.to_datetime(df['dt_ultima_compra']) #dt.date
        df['dt_primeira_compra'] = pd.to_datetime(df['dt_primeira_compra']) #dt.date
        df['dt_vencimento_fatura'] = pd.to_datetime(df['dt_vencimento_fatura']).dt.date
        df['dt_pagamento_ult_fatura'] = pd.to_datetime(df['dt_pagamento_ult_fatura']).dt.date
        df["vlr_fatura"]= pd.to_numeric(df.vlr_fatura, downcast="integer")
        df['fl_servico_sms'] = [1 if x == "S" else 0 for x in df["fl_servico_sms"]]
        df['fl_fatura_ativa_mes'] = [1 if x == "S" else 0 for x in df["fl_ativa"]]
        df = df.drop(["fl_ativa"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_parcelamento_fatura [['id_conta','fl_status']],left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df['fl_possui_parcelamento_fatura'] = [1 if x == True else 0 for x in pd.notnull(df["fl_status"])]
        df = df.drop(["fl_status"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['fl_possui_seguros'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros[(df_t_seguros['id_produto'] == 3)]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['fl_possui_odonto'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros[df_t_seguros['id_produto'].isin([1,2])]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df = df.drop_duplicates()
        df['fl_possui_fatura_garantida'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df.rename(columns={'nu_cpf_cnpj': 'id_pessoa'}, inplace=True)
        #df['id_pessoa']= [hashlib.md5(val.encode('utf-8')).hexdigest().upper() for val in df["nu_cpf_cnpj"]]
        #df = df.drop(["nu_cpf_cnpj"], axis=1) #=> EXCLUINDO COLUNA
        df = df[['id_pessoa','tx_status_cartao','tx_motivo_bloqueio','fl_possui_seguros','fl_possui_odonto',         
                'fl_possui_parcelamento_fatura','fl_possui_fatura_garantida','dt_ultima_compra','dt_primeira_compra',
                'dt_cadastro','qtd_dias_atraso','tx_loja_origem','vlr_fatura','vlr_pago', 'tx_status_conta','id_produto','tx_descricao_produto',
                'id_conta','dt_vencimento_fatura','fl_servico_sms','fl_fatura_ativa_mes', 'dt_pagamento_ult_fatura']]

        print (df.head(10))
      
        n_arquivo = ('dados_transacionais.parquet')

        df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        clientMinio.remove_object(PROCESSING, (past + n_arquivo))
        clientMinio.fput_object(PROCESSING, (past + n_arquivo), n_arquivo)

class InsereOracle(BaseOperator):
    def __init__(self, tb = str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tb = tb

    def execute(self, context):

        path = clientMinio.get_object(PROCESSING,past+n_arquivo)
        ds = pd.read_parquet(BytesIO(path.data))
        print (ds.head())
        df = pd.DataFrame(ds)

        rows = [tuple(x) for x in df.values]
        print(df.head())

        if len(df) < 1:
            print('Não há novos registros')
        else:
            print(df.head())
        
            columns = list(df.columns) #Os nomes de colunas do dataframe devem ser iguais aos da tabela de destino
            print(f'Total de Colunas Dataframe : {len(columns)}')

            print(df.columns)

            conn_crm = OracleHook(oracle_conn_id = "datawarehouse")
            conn_crm.bulk_insert_rows(table = "crm.stage_pessoa_cartao", 
            #conn_crm.bulk_insert_rows(table = "WANDERLEY_GONCALVES.Gpt_Integra_Cliente_Fila",
                                        rows = rows,
                                        target_fields = columns,
                                        commit_every = 10000)