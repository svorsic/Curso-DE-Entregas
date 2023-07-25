# Este es el DAG que orquesta el ETL de la tabla covid_colombia

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS covid_colombia (
    fecha_reporte_web DATETIME,
    id_de_caso INT,
    fecha_de_notificaci_n DATETIME,
    departamento INT,
    ciudad_municipio INT,
    edad INT,
    unidad_medida VARCHAR(50),
    sexo VARCHAR(50),
    fuente_tipo_contagio VARCHAR(50),
    ubicacion VARCHAR(50),
    estado VARCHAR(50),
    recuperado VARCHAR(50),
    fecha_inicio_sintomas DATETIME,
    fecha_diagnostico DATETIME,
    fecha_recuperado VARCHAR(50),
    tipo_recuperacion VARCHAR(50),
    per_etn_ INT,
    fecha_muerte VARCHAR(50),
    process_date VARCHAR(10) distkey
) SORTKEY(process_date, id_de_caso);
"""

QUERY_CLEAN_PROCESS_DATE = """
DELETE FROM covid_colombia WHERE process_date = '{{ ti.xcom_pull(key="process_date") }}';
"""


# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided take it, otherwise take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
    kwargs["ti"].xcom_push(key="process_date", value=process_date)


defaul_args = {
    "owner": "Santiago Vorsic",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_covid",
    default_args=defaul_args,
    description="ETL de la tabla covid Colombia",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
        dag=dag,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
        dag=dag,
    )

    clean_process_date = SQLExecuteQueryOperator(
        task_id="clean_process_date",
        conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
        dag=dag,
    )

    spark_etl_covid = SparkSubmitOperator(
        task_id="spark_etl_covid",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Covid.py',
        conn_id="spark_default",
        dag=dag,
        driver_class_path=Variable.get("driver_class_path"),
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_covid
