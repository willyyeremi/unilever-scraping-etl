from subprocess import Popen
from time import sleep
import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from sqlalchemy.engine.url  import URL


##################################################
# common used variable
##################################################

base_path = os.path.dirname(os.path.realpath(__file__))


##################################################
# connection to database
##################################################

driver_dict = {
    "postgresql": "psycopg2"
}
def create_url(config: dict[str:str], database_product: str):
    driver = driver_dict[database_product]
    url_object = URL.create(
        f"{database_product}+{driver}"
        ,username = config["username"]
        ,password = config["password"]
        ,host = config["host"]
        ,port = config["port"]
        ,database = config["schema"]
    )
    return url_object

conn_online_shop = BaseHook.get_connection("online_shop")
conn_config_online_shop = {
    "username": conn_online_shop.login,
    "password": conn_online_shop.password,
    "host": conn_online_shop.host,
    "port": conn_online_shop.port,
    "schema": conn_online_shop.schema
}
conn_url_online_shop = str(create_url(config = conn_config_online_shop, database_product = "postgresql"))


##################################################
# callable for dag
##################################################

def run_firefox_func():
    xvfb_process = Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'])
    sleep(5)

def raw_scrap_data_tokopedia_pipeline(connection_url):
    from sqlalchemy import create_engine
    from nodes.level_1 import raw_scrap_data_tokopedia
    
    conn_engine_online_shop = create_engine(connection_url)
    raw_scrap_data_tokopedia.run_pipeline(conn_engine_online_shop)


##################################################
# dag script and setup
##################################################

dag = DAG(
    dag_id = "unilever_scrapping_pipeline",
    dag_display_name  = " Unilever Scrapping Pipeline",
    tags = ["web-scrapping"],
    schedule_interval = None,
    start_date = datetime.datetime(2024, 1, 1),
    catchup = False,
    default_args = {
        "depends_on_past": False,
        "retries": 0,
        "email_on_failure": False,
        "email_on_retry": False,
    }
)

with dag:
    
    run_firefox = PythonOperator(
        task_id = 'run_firefox',
        python_callable = run_firefox_func
    )
    
    raw_scrap_data_tokopedia = PythonVirtualenvOperator(
        task_id = 'raw_scrap_data_tokopedia',
        python_callable = raw_scrap_data_tokopedia_pipeline,
        op_kwargs = {"connection_url": conn_url_online_shop},
        requirements = [r.strip() for r in open(os.path.join(base_path, "nodes", "level_1", "raw_scrap_data_tokopedia.txt")).readlines() if r.strip() and not r.strip().startswith("#")],
        system_site_packages = False,
    )
    
    run_firefox >> raw_scrap_data_tokopedia