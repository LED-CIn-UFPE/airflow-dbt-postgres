from datetime import datetime, timedelta
from airflow.decorators import dag, task
from include.football.extraction import extrair_dados, validar_dados
from include.football.loading import salvar_raw

DEFAULT_ARGS = {
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}


@dag(
  dag_id="pipeline_matches",
  start_date=datetime(2026, 1, 1),
  schedule="@weekly",
  catchup=False,
  default_args=DEFAULT_ARGS,
  tags=["futebol", "brasileirao", "partidas"],
)
def pipeline_matches():

    @task
    def task_extrair() -> dict:
      return extrair_dados(
        "/v4/competitions/BSA/matches",
        {"season": "2026"}
      )

    @task
    def task_validar(payload: dict) -> int:
      return validar_dados(payload, "matches")

    @task
    def task_salvar(payload: dict) -> None:
      salvar_raw(payload, "matches", "matches")

    payload = task_extrair()
    validado = task_validar(payload)
    salvo = task_salvar(payload)

    validado >> salvo


pipeline_matches()