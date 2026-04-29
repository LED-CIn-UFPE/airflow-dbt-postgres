from datetime import datetime, timedelta
from airflow.decorators import dag, task
from include.airflow_utils import wait_for_dag
from include.dbt_utils import dbt_run, dbt_test, dbt_deps

DEFAULT_ARGS = {
  "retries": 2,
  "retry_delay": timedelta(minutes=5),
}

@dag(
  dag_id="pipeline_transformacao",
  start_date=datetime(2026, 1, 1),
  schedule="@weekly",
  catchup=False,
  default_args=DEFAULT_ARGS,
  tags=["futebol", "brasileirao", "dbt", "mentoria"],
)
def pipeline_transformacao():

  wait_matches = wait_for_dag("pipeline_matches")
  wait_standings = wait_for_dag("pipeline_standings")
  wait_scorers = wait_for_dag("pipeline_scorers")

  @task
  def task_dbt_staging():
    dbt_run(select="staging")

  @task
  def task_dbt_marts():
    dbt_run(select="marts")

  @task
  def task_dbt_test():
    dbt_test()

  @task
  def task_dbt_deps():
    dbt_deps()

  deps = task_dbt_deps()
  staging = task_dbt_staging()
  marts = task_dbt_marts()
  test = task_dbt_test()

  [wait_matches, wait_standings, wait_scorers] >> deps >> staging >> marts >> test


pipeline_transformacao()