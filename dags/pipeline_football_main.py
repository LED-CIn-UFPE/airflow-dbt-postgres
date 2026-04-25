from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from include.football.extraction import extrair_partidas, validar_partidas
from include.football.loading import salvar_partidas

DEFAULT_ARGS = {
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}


@dag(
  dag_id="pipeline_futebol",
  start_date=datetime(2026, 1, 1),
  schedule="@daily",
  catchup=False,
  default_args=DEFAULT_ARGS,
    tags=["futebol", "brasileirao", "mentoria"],
)
def pipeline_futebol():

    @task
    def task_extrair() -> dict:
      return extrair_partidas()

    @task
    def task_validar(payload: dict) -> int:
      return validar_partidas(payload)

    @task
    def task_salvar(payload: dict) -> None:
      salvar_partidas(payload)

    payload = task_extrair()
    validado = task_validar(payload)
    salvo = task_salvar(payload)

    validado >> salvo


pipeline_futebol()