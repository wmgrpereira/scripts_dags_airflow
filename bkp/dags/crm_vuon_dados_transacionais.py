from datetime import datetime, timedelta
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API
from airflow.models.baseoperator import chain # A function that sets sequential dependencies between tasks including lists of tasks.
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule # Used to change how an Operator is triggered
from airflow.utils.task_group import TaskGroup
from airflow.operators.oracle_operator import OracleOperator
from crm.crm_vuon_dados_transacionais_functions import ConsultasHive, Processing, InsereOracle

#[START default_dag]
default_args = {
        "owner": "crm",
        "depends_on_post" : False, #se depende de post anterior
        #"email":,
        #"email_on_failure": False,
        #"email_on_retries": 1,
        "execution_timeout": timedelta(minutes=8),
        "timeout": 36000,
        "retries": 5,  # If a task fails, it will retry 1 times
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
def crm_vuon_dados_transacionais():

    init = DummyOperator(task_id="Init")
    with TaskGroup(group_id='Extracting') as Extrating_Hive:
        t_fatura = ConsultasHive(task_id = "t_fatura", tb = "t_fatura")
        t_anuidade_produto = ConsultasHive(task_id = "t_anuidade_produto", tb = "t_anuidade_produto")
        t_pagamento_fatura = ConsultasHive(task_id = "t_pagamento_fatura", tb = "t_pagamento_fatura")
        t_cliente = ConsultasHive(task_id="t_cliente", tb="t_cliente")
        t_conta = ConsultasHive(task_id="t_conta", tb="t_conta")
        t_seguros = ConsultasHive(task_id="t_seguros", tb="t_seguros")
        t_venda = ConsultasHive(task_id="t_venda", tb="t_venda")
        ult_fat_emitida = ConsultasHive(task_id="ult_fat_emitida", tb="ult_fat_emitida")
        t_parcelamento_fatura = ConsultasHive(task_id="t_parcelamento_fatura", tb="t_parcelamento_fatura")
        chain(ult_fat_emitida, [t_venda, t_fatura, t_pagamento_fatura], t_conta, [t_seguros, t_parcelamento_fatura, t_anuidade_produto], t_cliente)
    Processing_df = Processing(task_id = "Processing")#, tb = "t_cliente_gold")
    truncate_stg_pessoa_cartao= OracleOperator(task_id="truncate_stg_pessoa_cartao", sql = "truncate table crm.stage_pessoa_cartao", oracle_conn_id = "datawarehouse", autocommit = True)
    Insere_Oracle = InsereOracle(task_id = "Insere_Oracle", tb = 't_cliente_gold')
    finish = DummyOperator(task_id="Finish", trigger_rule=TriggerRule.NONE_FAILED)

    chain (init, Extrating_Hive, Processing_df, truncate_stg_pessoa_cartao, Insere_Oracle, finish)
dag = crm_vuon_dados_transacionais()