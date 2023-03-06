from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
#from airflow.operators.oracle_operator import OracleOperator
from airflow.hooks.hive_hooks import HiveServer2Hook
#from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.models.baseoperator import BaseOperator
#from minio.commonconfig import REPLACE, CopySource
from airflow.models import Variable
from minio import Minio
from os import getenv
from io import BytesIO
import pandas as pd
import hashlib
import os
import socket
#from airflow.operators.email import EmailOperator
#from airflow.operators.python import BranchPythonOperator
#from airflow.operators.weekday import BranchDayOfWeekOperator
#from airflow.utils.edgemodifier import Label # Used to label node edges in the Airflow UI
#from airflow.utils.task_group import TaskGroup # Used to group tasks together in the Graph view of the Airflow UI
#from airflow.utils.weekday import WeekDay # Used to determine what day of the week it is
#from dotenv import load_dotenv 
#from dags.crm.dados_transacionais.dados_transacionais_config import config_key
#from airflow.providers.oracle.hooks.oracle import OracleHook

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

        dataSets = Processing.listaMinio(bucket = DATA_LAKE_VUON,past = past)
        print(dataSets)

        for file in dataSets:
            titulo = file.replace(".parquet", "")
            path = clientMinio.get_object(DATA_LAKE_VUON,past+file)
            ds = pd.read_parquet(BytesIO(path.data))
            df_init = pd.DataFrame(ds)
            globals()[f'df_{titulo}'] = df_init
            print(f'df_{titulo} : {len(df_init)}')

        #função de processamento
        
        df_pagamento_fatura = df_t_pagamento_fatura[["id_fatura","id_conta","vl_pagamento","dt_pagamento"]]
        df_pagamento_fatura["total_pago"]= df_pagamento_fatura.groupby("id_fatura")["vl_pagamento"].transform('sum')#transform(lambda x: x.sum())
        df_pagamento_fatura["dt_ultimo_pagto"]= df_pagamento_fatura.groupby("id_fatura")["dt_pagamento"].transform('max')#transform(lambda y: y.max())
        df_pagamento_fatura = df_pagamento_fatura[["id_fatura","id_conta","total_pago","dt_ultimo_pagto"]]
        df_pagamento_fatura = df_pagamento_fatura.drop_duplicates()
        df = df_t_fatura[["id_fatura", "id_conta", "dt_vencimento", "vl_fatura", "fl_envia_sms_directone","fl_ativa"]].merge(
        df_pagamento_fatura, left_on=["id_fatura","id_conta"], right_on = ["id_fatura","id_conta"], how = 'left')
        df = df.drop_duplicates()
        df = df.merge(df_t_conta[["id_conta", "id_produto","nu_cpf_cnpj","qtddiasatraso"]], left_on="id_conta", right_on="id_conta", how = "inner")
        df = df.merge(df_t_anuidade_produto[["id_produto","vl_faturamento_minimo"]],
                       left_on="id_produto", right_on="id_produto", how = "inner")
        df = df[(df.vl_fatura > df.vl_faturamento_minimo)]
        df['dt_vencto'] = pd.to_datetime(df['dt_vencimento']).dt.date
        df['dt_ult_pagto'] = pd.to_datetime(df['dt_ultimo_pagto']).dt.date
        df["vl_fatura"]= pd.to_numeric(df.vl_fatura, downcast="integer")
        df = df[['id_fatura', 'id_conta', 'dt_vencto', 'vl_fatura', 'total_pago','dt_ult_pagto',
                 'fl_envia_sms_directone','nu_cpf_cnpj', 'qtddiasatraso','fl_ativa']]
        df['flServicoSMS'] = [1 if x == "S" else 0 for x in df["fl_envia_sms_directone"]]
        df = df.drop(["fl_envia_sms_directone"], axis=1) #=> EXCLUINDO COLUNA
        df = df.rename(columns={'flServicoSMS': 'fl_envia_sms_directone'})
        #df_odonto = df_t_seguros[df_t_seguros.id_produto == 3]
        #df_fatura_garantida = df_t_seguros[df_t_seguros.id_produto.isin([1,2])]
        df = df.merge(
        df_t_parcelamento_fatura [['id_conta','fl_status']], left_on = 'id_conta', right_on = 'id_conta', how = 'left')
        df['fl_possui_parcelamento_fatura'] = [1 if x == True else 0 for x in pd.notnull(df["fl_status"])]
        df = df.drop(["fl_status"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['flgPossuiSeguros'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_seguros[(df_t_seguros.id_produto == 3)]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df['flgPossuiOdonto'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        df = df.merge(df_t_cliente[["id_conta", "nu_cpf_cnpj","fl_status_conta"]] , left_on = ["id_conta", "nu_cpf_cnpj"], right_on = ["id_conta", "nu_cpf_cnpj"], how = 'left')
        df = df.merge(df_t_seguros[df_t_seguros.id_produto.isin([1,2])]['cpf'], left_on = 'nu_cpf_cnpj', right_on = 'cpf', how = 'left')
        df = df.drop_duplicates()
        df['flgPossuiFaturaGarantida'] = [1 if x == True else 0 for x in pd.notnull(df["cpf"])]
        df = df.drop(["cpf"], axis=1) #=> EXCLUINDO COLUNA
        print (df.head(10))
        # print(f'\n\
        # Possui SMS: {len(df[df.fl_envia_sms_directone==1])} \n\
        # Possui Odonto: {len(df[df.flgPossuiOdonto==1])} \n\
        # Possui Seguros: {len(df[df.flgPossuiSeguros.isin([1])])} \n\
        # Possui Fatura Garantida: {len(df[df.flgPossuiFaturaGarantida.isin([1])])}')
        df['id_pessoa']= [hashlib.md5(val.encode('utf-8')).hexdigest().upper() for val in df["nu_cpf_cnpj"]]
        df = df.drop(["nu_cpf_cnpj"], axis=1) #=> EXCLUINDO COLUNA

        n_arquivo = ('dados_transacionais.parquet')

        #df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        clientMinio.remove_object(PROCESSING, (past + n_arquivo))
        clientMinio.fput_object(PROCESSING, (past + n_arquivo), n_arquivo)




#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":,
        #"email_on_failure": False,
        #"email_on_retries": 1,
        "execution_timeout": timedelta(minutes=15),
        "timeout": 36000,
        "retries": 3,  # If a task fails, it will retry 1 times
        "retry_delay": timedelta(minutes=1) #rodar novamente em caso de falha
    }

@dag(
    start_date=datetime(2022, 11, 1),
    max_active_runs=1,
    #schedule_interval= "32 13 * * *",
    #schedule_interval="@daily",
    #schedule_interval= 1 * * * *,
    default_view="graph",
    catchup=False,
    default_args = default_args,
    tags=["crm", "dados_trasacionais"], # If set, this tag is shown in the DAG view of the Airflow UI
)
def crm_dados_transacionais():

    init = DummyOperator(task_id="Init")
    #truncate_stg_venda = OracleOperator(task_id="truncate_stg_venda", sql = "truncate table crm.stg_venda", oracle_conn_id = "datawarehouse", autocommit = True)
    with TaskGroup(group_id='Extracting') as Extrating_Hive:
        t_fatura = ConsultasHive(task_id = "t_fatura", tb = "t_fatura")
        t_anuidade_produto = ConsultasHive(task_id = "t_anuidade_produto", tb = "t_anuidade_produto")
        t_pagamento_fatura = ConsultasHive(task_id = "t_pagamento_fatura", tb = "t_pagamento_fatura")
        #t_cliente = ConsultasHive(task_id="t_cliente", tb="t_cliente")
        t_conta = ConsultasHive(task_id="t_conta", tb="t_conta")
        t_seguros = ConsultasHive(task_id="t_seguros", tb="t_seguros")
        t_venda = ConsultasHive(task_id="t_venda", tb="t_venda")
        ult_fat_emitida = ConsultasHive(task_id="ult_fat_emitida", tb="ult_fat_emitida")
        t_parcelamento_fatura = ConsultasHive(task_id="t_parcelamento_fatura", tb="t_parcelamento_fatura")
        #chain(t_fatura)
        chain(ult_fat_emitida, [t_venda, t_fatura, t_pagamento_fatura], t_conta, [t_seguros, t_parcelamento_fatura, t_anuidade_produto])
    #Processing_df = Processing(task_id = "Processing")#, tb = "t_cliente_gold")
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    chain (init, Extrating_Hive, finish)
    #   chain (t_parcelamento_fatura)
dag = crm_dados_transacionais()


def config_task(name_task):
    tasks = {
        "t_anuidade_produto":{
            "sql": """SELECT p.ds_produto as ds_produto,
                            p.id_produto as id_produto,
                            p.vl_faturamento_minimo as vl_faturamento_minimo
                        FROM vuon_db1.t_anuidade_produto p"""
        },
        "t_fatura":{
            "sql": """SELECT i.id_fatura as id_fatura,
					i.id_conta as id_conta,
					i.dt_vencimento as dt_vencimento_fatura,
					i.fl_ativa as fl_ativa,
					i.fl_envia_sms_directone as fl_servico_sms,
					i.vl_fatura as vlr_fatura
                    FROM
                        vuon_db1.t_fatura i
                    WHERE
                    YEAR(i.dt_vencimento) || MONTH (i.dt_vencimento) =
                    YEAR(from_unixtime(unix_timestamp())) || MONTH(from_unixtime(unix_timestamp()))"""
        },
        "t_seguros":{
            "sql":"""select tkr.vl_resposta as CPF,
                            tka.id_produto as id_produto
                        from vuon_db1.t_klok_campo    tkc,
                            vuon_db1.t_klok_resposta tkr,
                            vuon_db1.t_klok_adesao   tka
                        where tkc.ds_codigo = 'contratante.dadosPessoais.cpf'
                        and tkc.id_campo = tkr.id_campo
                        and tkr.id_adesao = tka.id_adesao
                        and tka.ds_status IN ('ATIVA', 'PENDENTE_CONFIRMACAO_PROVEDOR', 'INCONSISTENTE')
                        and tka.id_produto IN  ('1','2','3', '4','5')"""
        },
        "t_venda":{
            "sql": """SELECT tv.id_conta as id_conta,
                            max(tv.dt_venda) as dt_ultima_compra,
                            min(tv.dt_venda) as dt_primeira_compra
                        FROM vuon_db1.t_venda tv
                        WHERE tv.fl_status = 'A'
                        GROUP BY tv.id_conta"""
        },
        "t_pagamento_fatura":{
            "sql": """select --f.id_pagamento_fatura     as id_pagamento_fatura,
                             f.id_fatura               as id_fatura,
                             f.id_conta                as id_conta,
                             f.dt_pagamento            as dt_pagamento_ult_fatura,
                             f.vl_pagamento            as vlr_pago
                            from vuon_db1.t_pagamento_fatura f
                            JOIN (SELECT a.id_conta as id_conta, max(a.id_fatura) as id_fatura
                                    FROM vuon_db1.t_fatura a
                                    JOIN vuon_db1.t_conta x
                                        ON x.id_conta = a.id_conta
                                    JOIN vuon_db1.t_anuidade_produto Y
                                        ON y.id_produto = x.id_produto
                                    WHERE a.vl_fatura > y.vl_faturamento_minimo
                                    GROUP BY a.id_conta) v
                            ON f.id_fatura = v.id_fatura"""
        },
        "ult_fat_emitida":{
            "sql":"""select vw.id_conta as id_conta,
						vw.id_fatura  as id_fatura
					from vuon_db1.vw_ultima_fatura vw"""
        },
        "t_conta":{
            "sql":"""select id_conta,
                         nu_cpf_cnpj as nu_cpf_cnpj,
                         ds_status_conta as tx_status_conta,
                         nm_origem_comercial as tx_loja_origem,
                         ds_status_cartao as tx_status_cartao,
                         ds_motivo_bloqueio_cartao as tx_motivo_bloqueio,
                         id_produto as id_produto,
                         nm_produto as tx_descricao_produto,
                         nu_dias_atraso as qtd_dias_atraso
                        from vuon_db1.t_conta"""
        },
        "t_cliente":{
            "sql":"""select --id_cliente,
                            id_conta,
                            --nm_cliente,
                            --nu_rg,
                            --nu_cpf_cnpj,
                            -- nu_insc_estadual,
                            dt_cadastro as dt_cadastro
                        from vuon_db1.t_cliente"""
        },
        "t_klok_adesao_dependente":{
            "sql":"""select id_adesao_titular,
                            id_adesao_dependente
                        from vuon_db1.t_klok_adesao_dependente"""
        },
        "t_parcelamento_fatura":{
            "sql":"""SELECT
                        tp.id_conta						as	id_conta,
                        tp.fl_status					as	fl_status
                    FROM
                        vuon_db1.t_parcelamento_fatura_acid tp
                    where
                        tp.fl_status = 'A'"""
        }
    }
    return tasks[name_task]

    