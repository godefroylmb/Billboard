from airflow import DAG, Dataset
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import asyncio
import os

from billboard.scripts.scraper import get_data
from billboard.scripts.db_manager import insert_rows, export_table_to_csv
from billboard.scripts.config import load_charts
from billboard.scripts.kaggle_uploader import upload_export_folder_to_kaggle

charts = load_charts()

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def get_oldest_date():
    dates = [datetime.fromisoformat(c["start_date"]) for c in charts.values()]
    return min(dates)

billboard_dataset = Dataset("billboard_data")

with DAG(
    dag_id="billboard_dag",
    start_date=get_oldest_date(),
    schedule="0 3 * * 3",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["billboard", "weekly"],
) as dag:

    @task(retries=3)
    def scrap_chart(chart_name, chart_conf):
        context = get_current_context()
        ds = context["ds"]

        if datetime.fromisoformat(ds) < datetime.fromisoformat(chart_conf["start_date"]):
            return None

        async def run():
            rows = await get_data(date_str=ds, charts=chart_conf["url_path"])
            return rows

        return asyncio.run(run())

    @task_group(group_id="scrap")
    def scrap_group():
        outputs = {}
        for name, conf in charts.items():
            outputs[name] = scrap_chart.override(task_id=name)(name, conf)
        return outputs


    @task(retries=0)
    def insert_chart(rows, chart_conf):
        if not rows:
            return
        insert_rows(table=chart_conf["table"], rows=rows)

    @task_group(group_id="insert")
    def insert_group(scrap_outputs):
        tasks = {}
        for name, conf in charts.items():
            tasks[name] = insert_chart.override(task_id=name)(scrap_outputs[name], conf)
        return tasks


    @task
    def export_csv_task(chart_name, chart_conf):        
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        export_dir = os.path.join(dag_dir, "billboard", "export")
        os.makedirs(export_dir, exist_ok=True)
        
        csv_path = os.path.join(export_dir, f"{chart_name}.csv")
        
        export_table_to_csv(table=chart_conf["table"], output_path=csv_path)
        
        return csv_path

    @task_group(group_id="export")
    def export_group(insert_outputs):
        tasks = {}
        for name, insert_task in insert_outputs.items():
            tasks[name] = export_csv_task.override(task_id=f"export_csv_{name}")(name, charts[name])
            insert_task >> tasks[name]
        return tasks    


    @task(outlets=[billboard_dataset])
    def upload_to_kaggle(_files):
        dag_dir = os.path.dirname(os.path.abspath(__file__))
        export_dir = os.path.join(dag_dir, "billboard", "export")

        kaggle_api_token = Variable.get("KAGGLE_API_TOKEN", default_var="")
        dataset_ref = Variable.get("BILLBOARD_URL", default_var="")

        if not kaggle_api_token or not dataset_ref:
            raise ValueError("Both KAGGLE_API_TOKEN and BILLBOARD_URL must be set.")

        context = get_current_context()
        version_notes = f"Airflow run {context['run_id']} - {context['ds']}"

        upload_result = upload_export_folder_to_kaggle(
            export_dir=export_dir,
            dataset_ref=dataset_ref,
            kaggle_api_token=kaggle_api_token,
            version_notes=version_notes,
        )

        return {
            "dataset": dataset_ref,
            "upload_result": upload_result,
        }

    scrap_outputs = scrap_group()
    insert_tasks = insert_group(scrap_outputs)
    export_tasks = export_group(insert_tasks)

    upload = upload_to_kaggle(list(export_tasks.values()))