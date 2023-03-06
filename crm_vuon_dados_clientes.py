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
DATA_LAKE_VUON = getenv("DATA_LAKE_VUON","data-lake-vuon" if AMBIENTE == 'PROD' else "data-lake-vuon-dev" )

clientMinio = Minio(MINIO,ACESS_KEY,SECRET_ACCESS, secure=False)
past = 'crm/clientes_vuon/'
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

class ConsultasOracle(BaseOperator):
    def __init__(self, tb = str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.tb = tb

    def execute(self, context):

        config = config_task (self.tb)#("step_stg_venda")
        sql = config["sql"]
        n_arquivo = (f'{self.tb}.parquet')
        #print(sql)
        oracle_hook = OracleHook(oracle_conn_id='datawarehouse')
        consulta = oracle_hook.get_records(sql=sql)
        #self.xcom_push(context, "sequence", n_arquivo)
        print(f'Consulta: {consulta[0][0]}')
        #self.xcom_push(context, self.tb, consulta)
        df = pd.DataFrame(consulta)
        df.columns = df.columns.astype(str)
        print(df.head())

        df = df.to_parquet(n_arquivo, compression='gzip') #Converte para parquet
        clientMinio.remove_object(DATA_LAKE_VUON, (past + n_arquivo))
        clientMinio.fput_object(DATA_LAKE_VUON, (past + n_arquivo), n_arquivo)
        return (consulta)

    def Get_Sequence (procedure, p_num, tb):

        oracle_hook = OracleHook(oracle_conn_id='datawarehouse')
        oracle_hook.callproc(identifier = procedure, parameters = p_num)
                
        config = config_task (tb)#("step_stg_venda")
        sql = config["sql"]
        #n_arquivo = (f'{self.tb}.parquet')
        #print(sql)
        oracle_hook = OracleHook(oracle_conn_id='datawarehouse')
        sequence = oracle_hook.get_records(sql=sql)
        #print(sequence)
        seq = [x[0] for x in sequence]
        #self.xcom_push(context, "Sequence", sequence)
        return seq

    # def sequence(tb, df):
    #     consulta_lista = []
    #     for x in range(len(df)):
    #         config = config_task (tb)#("step_stg_venda")
    #         sql = config["sql"]
    #         #print(sql)
    #         oracle_hook = OracleHook(oracle_conn_id='datawarehouse')
    #         consulta = oracle_hook.get_records(sql=sql)
    #         consulta_lista.append(consulta[0][0])
    #     return(consulta_lista)
    
    # def seqcidade(cep):
    #     sql = [f'SELECT h.seqcidade FROM Consinco.Ge_Cidade@c5 h WHERE {cep} BETWEEN h.Cepinicial AND h.Cepfinal AND Rownum = 1']
    #     print(sql)
    #     oracle_hook = OracleHook(oracle_conn_id='datawarehouse')
    #     seq = oracle_hook.get_records(sql=sql)
    #     seq = seq[0][0][0]
    #     return(seq)

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
        #self.xcom_push(context, self.tb, n_arquivo)
        
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
        
        #teste = context['task_instance'].xcom_pull("t_telefone", key = "t_telefone")
        #Sequence = context['task_instance'].xcom_pull("Consulta_Oracle", key="sequence")
        # inicio = Sequence[0][0]
        # print(f'Tipo de dado {inicio} - {type(inicio)}')

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
        df = df_t_cliente_gold.merge(df_t_cliente_cadastro, left_on="id_conta", right_on="id_conta", how = "inner")
        #df = df.drop(["id_conta"], axis=1) #=> EXCLUINDO COLUNA
        print (df.head())
        df_telefone= df_t_telefone.sort_values(by =['id_conta','fone_celular'], axis=0, ascending=[True, False], ignore_index=True)
        df_telefone['rank']= df_telefone.groupby(['id_conta']).cumcount() + 1
        df1 = df_telefone[df_telefone['rank'] == 1]
        col_df1 = {'fone_celular':'fone_celular_1'}
        df1 = df1.rename(columns=col_df1)
        df2 = df_telefone[df_telefone['rank'] == 2]
        col_df2 = {'fone_celular':'fone_celular_2'}
        df2 = df2.rename(columns=col_df2)
        df_telefone = df1[['id_conta', 'fone_celular_1']].merge(df2[['id_conta', 'fone_celular_2']], left_on="id_conta", right_on="id_conta", how = "left")
        print(df_telefone.head())
        df = df.merge(df_telefone, left_on="id_conta", right_on="id_conta", how = "inner")
        df = df.drop_duplicates()
        print (df.head())
   

        df['dtainclusao'] = [datetime.strptime(x, '%Y-%m-%d').date() for x in df['dtainclusao'].str[:10]]
        df = df[(df.dtainclusao >= datetime.date(datetime.today() - timedelta(days = 2)))] # LIMITAÇÃO DE DIAS PARA SELEÇÃO
        df['dtanascfund'] = [datetime.strptime(x, '%Y-%m-%d').date() for x in df['dtanascfund']]
        df['data_selecao'] = [datetime.strptime(x, '%Y-%m-%d').date() for x in df['data_selecao']]
        df['digcgccpf'] = [x[-2:] for x in df['cpf_cnpj']]
        df['nrocgccpf'] = [x[:-2] for x in df['cpf_cnpj']]
        df['digcgccpf'] = pd.to_numeric(df['digcgccpf'])
        df['nrocgccpf'] = pd.to_numeric(df['nrocgccpf'])
        df['bairro'] = df['bairro'].str[:50]   #Limitando de acordo com a capacidade da tabela
        #df['atividade'] = df['atividade'].str[:35]   #Limitando de acordo com a capacidade da tabela
        df['cidade'] = df['cidade'].str[:30]   #Limitando de acordo com a capacidade da tabela
        df['logradouro'] = df['logradouro'].str[:80]   #Limitando de acordo com a capacidade da tabela
        df['foneddd1'] = [x[:2] if (pd.notnull(x) == True) and (len(x) >= 11) else '' for x in df['fone_celular_1']]
        df['fonenro1'] = [x[2:] if (pd.notnull(x) == True) and (len(x) >= 11) else '' for x in df['fone_celular_1']]
        df['foneddd2'] = [x[:2] if (pd.notnull(x) == True) and (len(x) >= 11) else '' for x in df['fone_celular_2']]
        df['fonenro2'] = [x[2:] if (pd.notnull(x) == True) and (len(x) >= 11) else '' for x in df['fone_celular_2']]

        df = df.drop(["id_conta", "fone_celular_1", "fone_celular_2"], axis=1) #=> EXCLUINDO COLUNA INUTILIZADAS
          
        #df['seqcidade'] = [ConsultasOracle.seqcidade(cep = x) for x in df['cep']]
        #print(df[['seqcidade','cep', 'cidade']].head(10))
        parameters = {'p_num': str(len(df))}
        #df['id_pessoa_fila'] = ConsultasOracle.Get_Sequence(procedure="crm.p_sequence_vuon_ingegra_c5", p_num=str(len(df)), tb= 'sequence')
        df['id_pessoa_fila']= ConsultasOracle.Get_Sequence(procedure="crm.p_sequence_vuon_ingegra_c5", p_num=parameters, tb= 'sequence')
        #print(len(teste))

        #df['id_fila'] = [x for x in range(0, len(df))]
        # teste = ConsultasOracle.sequence(tb = 'gera_id_fila', df = df)
        # print(f'Resultado do teste: {teste}')

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

            print(f'Total de Colunas Oracle : {len(columns)}')
            conn_crm = OracleHook(oracle_conn_id = "datawarehouse")
            conn_crm.bulk_insert_rows(table = "gerencial.Gpt_Integra_Cliente_Fila@c5", 
            #conn_crm.bulk_insert_rows(table = "WANDERLEY_GONCALVES.Gpt_Integra_Cliente_Fila",
                                        rows = rows,
                                        target_fields = columns,
                                        commit_every = 10000)


#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":"wanderley.goncalves@grpereira.com.br",
        #"email_on_failure": False,
        #"email_on_retries": 1,
        "execution_timeout": timedelta(minutes=15),
        "retries": 3,  # If a task fails, it will retry 1 times
        "retry_delay": timedelta(minutes=1) #rodar novamente em caso de falha
    }

@dag(
    start_date=datetime(2022, 11, 1),
    max_active_runs=1, #total de execuções simultâneas
    schedule_interval= "30 13 * * *",
    #schedule_interval= "30 12 * * 1,2,3,4,5", #De seg a sex, às 08:30h
    #schedule_interval="@daily",
    #schedule_interval=timedelta(hours=24),
    default_view="graph",
    catchup=False,
    default_args = default_args,
    tags=["crm", "dados_clientes_vuon"], # If set, this tag is shown in the DAG view of the Airflow UI
)

def crm_vuon_dados_clientes():

    init = DummyOperator(task_id="Init")
    #truncate_stg_venda = OracleOperator(task_id="truncate_stg_venda", sql = "truncate table crm.stg_venda", oracle_conn_id = "datawarehouse", autocommit = True)
    with TaskGroup(group_id='Extracting') as Extrating_Hive:
        t_cliente_gold= ConsultasHive(task_id = "t_cliente_gold", tb = "t_cliente_gold")#, do_xcom_push=True)
        t_cliente_raw= ConsultasHive(task_id = "t_cliente_cadastro", tb = "t_cliente_cadastro")
        t_telefone_gold= ConsultasHive(task_id = "t_telefone", tb = "t_telefone")
        t_gecidade = ConsultasOracle(task_id = "t_gecidade", tb = 't_gecidade')
        chain([t_cliente_gold, t_cliente_raw, t_telefone_gold, t_gecidade])
    #ConsultaOracle = ConsultasOracle(task_id = "Consulta_Oracle", tb = 'gera_id_fila', do_xcom_push=True)
    Processing_df = Processing(task_id = "Processing")#, tb = "t_cliente_gold")
    Insere_Oracle = InsereOracle(task_id = "Insere_Oracle", tb = 't_cliente_gold')
    #teste = PythonOperator(task_id = "teste", python_callable= captura_conta_dados)
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    chain(init, Extrating_Hive, Processing_df, Insere_Oracle, finish)

#Instanciando a dag:
dag = crm_vuon_dados_clientes()

def config_task(name_task):
    tasks = {
        "t_cliente_gold":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT null               as id_pessoa_fila, 
                            'VUON' 				as Edi_Source,  		
                            f.nm_cliente 		as Nomerazao,			
                            f.nm_cliente 		as Fantasia,
                            f.ds_email 			as Email,
                            'CLIENTE' 			as Palavrachave,
                            'BRASIL' 			as Pais,
                            f.cd_uf 			as Uf,
                            f.nm_bairro 		as Bairro,
                            f.nm_logradouro		as Logradouro,
                            f.nu_cep			as Cep,
                            f.nu_endereco		as Nrologradouro,
                            f.nu_cpf_cnpj 		as Cpf_cnpj,
                            null                as Digcgccpf,
                            f.dt_nascimento 	as Dtanascfund,
                            'VUON' 				as Origem,
                            'HIVE' 				as Usuinclusao,
                            f.fl_sexo 			as Sexo,
                            f.nm_cidade 		as Cidade,
                            f.fl_tipo_pessoa 	as Fisicajuridica,
                            f.nu_rg 			as Inscricaorg,
                            to_date(from_unixtime(unix_timestamp(CURRENT_DATE, 'yyyyMMdd'),"yyyy-MM-dd")) as Data_Selecao,
                            f.id_conta 			as id_conta,
                            'I'                 as Tipo_Integracao,
                            'F'                 as Status_Integracao
                            FROM vuon_db1_gold.t_dim_cliente f
                            --WHERE f.id_conta BETWEEN '811800' and '811900'
                            """
        },
        "t_cliente_cadastro":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT c.dt_cadastro 		as Dtainclusao,
							c.id_conta 			as id_conta
							FROM vuon_db1_gold.t_fat_cliente_cadastro c
							--WHERE c.id_conta BETWEEN '811800' and '811900'
                            """
        },
        "t_telefone":{
            "sql": """SELECT t.id_conta			as id_conta,
				             t.telefone_celular 		as fone_celular
				             --,t.telefone_residencial 	as fone_residencial
				             FROM vuon_db1_gold.t_fat_telefone t
                        """
        },
        "t_gecidade":{
            "sql": """SELECT h.seqcidade, h.Cepinicial, h.Cepfinal, h.Cidade FROM Consinco.Ge_Cidade@c5 h
                        WHERE h.codmunicipio IS NOT null"""
        },
        "sequence":{
            "sql": """select t.id_fila from crm.stg_sequence_vuon_integra t"""
        }
    }
    
    return tasks[name_task]