from time import sleep
import datetime
import os

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.decorators import task_group, task
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

@task.virtualenv(
    task_id = 'raw_scrap_data_tokopedia',
    requirements = [r.strip() for r in open(os.path.join(base_path, "nodes", "level_1", "raw_scrap_data_tokopedia.txt")).readlines() if r.strip() and not r.strip().startswith("#")],
    system_site_packages = False,
)
def raw_scrap_data_tokopedia(connection_url, real_base_path):
    import sys
    from time import sleep
    from subprocess import Popen
    from sqlalchemy import create_engine
    sys.path.append(real_base_path)
    from nodes.level_1 import raw_scrap_data_tokopedia
    
    with Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24']) as xvfb_process:
        os.environ["DISPLAY"] = ":99" 
        sleep(5)
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
    
    @task_group()
    def level_1():
        raw_scrap_data_tokopedia_pipeline_task = raw_scrap_data_tokopedia(conn_url_online_shop, base_path)
    
    level_1()