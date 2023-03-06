from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
from crm.crm_vuon_integracao_clientes_functions import ConsultasHive, Processing, ConsultasOracle, InsereOracle

#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":"wanderley.goncalves@grpereira.com.br",
        #"email_on_failure": False,
        #"email_on_retries": 1,
        "execution_timeout": timedelta(minutes=8),
        "retries": 5,  # If a task fails, it will retry 1 times
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
    tags=["crm", "integração_clientes_vuon"], # If set, this tag is shown in the DAG view of the Airflow UI
)

def crm_vuon_integracao_clientes():

    init = DummyOperator(task_id="Init")
    #truncate_stg_venda = OracleOperator(task_id="truncate_stg_venda", sql = "truncate table crm.stg_venda", oracle_conn_id = "datawarehouse", autocommit = True)
    with TaskGroup(group_id='Extracting') as Extrating_Hive:
        t_cliente_gold= ConsultasHive(task_id = "t_cliente_gold", tb = "t_cliente_gold")#, do_xcom_push=True)
        t_cliente_raw= ConsultasHive(task_id = "t_cliente_cadastro", tb = "t_cliente_cadastro")
        t_telefone_gold= ConsultasHive(task_id = "t_telefone", tb = "t_telefone")
        t_gecidade = ConsultasOracle(task_id = "t_gecidade", tb = 't_gecidade')
        chain([t_cliente_gold, t_cliente_raw, t_telefone_gold, t_gecidade])
    with TaskGroup(group_id='Transform') as Transform:
        Processing_df = Processing(task_id = "Processing")#, tb = "t_cliente_gold")
        chain(Processing_df)
    with TaskGroup(group_id='Transform') as Load:
        Insere_Oracle = InsereOracle(task_id = "Insere_Oracle", tb = 't_cliente_gold')
        chain(Insere_Oracle)
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)
    chain(init, Extrating_Hive, Transform, Load, finish)

#Instanciando a dag:
dag = crm_vuon_integracao_clientes()
