import pendulum
import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago


def create_dag(DAG_NAME, dag_element): 
    
        #Environment variables
        config_yaml_path = f'/home/airflow/gcs/dags/templated-airflow-dag/config/{dag_element.lower()}/{dag_element.lower()}.yaml'
               
        # Job variables
        cst = pendulum.timezone("America/Chicago")
        
        file = open(config_yaml_path, "r") 
        ymlData = yaml.load(file, Loader=yaml.FullLoader)
        
        # Define default args
        default_args = {
           'owner': 'Airflow',
           'start_date': cst.convert(days_ago(0))}

        dag = DAG(
           tags=[dag_element.lower()],
           dag_id=DAG_NAME,
           description=f"Dynamically generated DAG for {dag_element}",
           default_args=default_args,
           template_searchpath = f'/home/airflow/gcs/dags/templated-airflow-dag/sql/{dag_element.lower()}/',
           schedule_interval=None,
           catchup=False,
           )
        
        
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")
        task_map = {}

        # Create tasks
        for task_conf in ymlData["tasks"]:
            task_id = task_conf["task_id"]
            sql_file = task_conf["sql"]

            bq_task = BigQueryInsertJobOperator(
                task_id=task_id,
                dag=dag,
                configuration={
                     "query": {
                         "query": f"{{% include '{sql_file}' %}}",
                         "useLegacySql": False,
        }
    },
                location="US",
        )

            task_map[task_id] = bq_task
            
        
        # Set dependencies
        for task_conf in ymlData["tasks"]:
            task_id = task_conf["task_id"]
            
            
            if 'depends_on' in task_conf:
                dep_task_ids = task_conf['depends_on']
                
                # Handle list or single string
                if isinstance(dep_task_ids, list):
                   for dep_task_id in dep_task_ids:
                       task_map[dep_task_id] >> task_map[task_id]
                else:
                    task_map[dep_task_ids] >> task_map[task_id]
                    
        all_task_ids = [task_conf["task_id"] for task_conf in ymlData["tasks"]]
        for task_id in all_task_ids:
            if "depends_on" not in next(t for t in ymlData["tasks"] if t["task_id"] == task_id):
                start >> task_map[task_id]
            if not any(task_id in t.get("depends_on", []) for t in ymlData["tasks"]):
                task_map[task_id] >> end
                               
        return dag            
            

dags_list = ["Attribution", "BPCI_A"]
for dag_element in dags_list:
    DAG_NAME = f'vic_{dag_element.lower()}_bqload'
    globals()[DAG_NAME] = create_dag(DAG_NAME, dag_element)