import pandas as pd
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from src.exchange_rate.exporter import ParseExchangeRate

DAG_ID = "HISTORY-PARSE_PAIRS_FROM_EXCHANGERATE"
DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=60),
    "depends_on_past": False,
    "provide_context": True,
}
SCHEDULE_INTERVAL = None
TAGS = ["exchange_rate", "history"]


def run_sql_query_in_ch(**context):
    ch_hook = ClickHouseHook()
    ch_hook.run('CREATE DATABASE IF NOT EXISTS EXCHANGE_RATE')
    with open(context['query'], 'r') as sql:
        ch_sql = sql.read().replace('\n', '')
    ch_hook.run(ch_sql)


def stg_load_to_ch(**context):
    ch_hook = ClickHouseHook()

    df = ParseExchangeRate.export_data_from_source(
        date_fmt="date",
        history=True,
        currency_from="BTC",
        currency_to="USD",
        start_date=context["start_date"],
        end_date=context["end_date"],
    )
    data = list(df.itertuples(index=False, name=None))
    ch_hook.run("INSERT INTO EXCHANGE_RATE.stg_pairs_exchange_rate VALUES", data)

def hs_load_to_ch(**context):
    import pandas as pd
    
    ch_hook = ClickHouseHook()
    
    for date in pd.date_range(start=context['start_date'], end=context['end_date']):
        ch_hook.run(f"ALTER TABLE EXCHANGE_RATE.core_pairs_exchange_rate DROP PARTITION {date.strftime('%Y%m%d')}")
    
    ch_hook.run(f"""INSERT INTO EXCHANGE_RATE.core_pairs_exchange_rate 
                    SELECT  pair
                           ,parseDateTimeBestEffortOrNull(export_date) AS export_date
                           ,rate 
                    FROM EXCHANGE_RATE.stg_pairs_exchange_rate
                """)

def truncate_stg_table(**context):
    ch_hook = ClickHouseHook()
    ch_hook.run('TRUNCATE TABLE IF EXISTS EXCHANGE_RATE.stg_pairs_exchange_rate')



with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=TAGS,
) as dag:

    create_stg_table_in_ch = PythonOperator(
        task_id="create_stg_table_in_ch",
        python_callable=run_sql_query_in_ch,
        op_kwargs={
            "query": "/usr/local/airflow/dags/sql/create_stg_table_in_ch.sql",
        },
        dag=dag,
    )

    create_core_table_in_ch = PythonOperator(
        task_id="create_core_table_in_ch",
        python_callable=run_sql_query_in_ch,
        op_kwargs={
            "query": "/usr/local/airflow/dags/sql/create_core_table_in_ch.sql",
        },
        dag=dag,
    )

    stg_load_to_ch = PythonOperator(
        task_id="stg_load_to_ch",
        python_callable=stg_load_to_ch,
        op_kwargs={
            "start_date": "2010-01-01",
            "end_date": "2010-02-01"
        },
        dag=dag,
    )

    hs_load_to_ch = PythonOperator(
        task_id="hs_load_to_ch",
        python_callable=hs_load_to_ch,
        op_kwargs={
            "start_date": "2010-01-01",
            "end_date": "2010-02-01"
        },
        dag=dag,
    )

    truncate_stg_table = PythonOperator(
        task_id="truncate_stg_table",
        python_callable=truncate_stg_table,
        dag=dag,
    )

(
    [create_stg_table_in_ch, create_core_table_in_ch] 
    >> stg_load_to_ch 
    >> hs_load_to_ch 
    >> truncate_stg_table
)

