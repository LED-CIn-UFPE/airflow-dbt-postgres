from datetime import datetime, timedelta
from airflow.decorators import dag, task
from include.football.extraction import extrair_dados, validar_dados
from include.football.loading import salvar_raw

DEFAULT_ARGS = {
  "retries": 3,
  "retry_delay": timedelta(minutes=5),
}


@dag(
  dag_id="pipeline_scorers",
  start_date=datetime(2026, 1, 1),
  schedule="@weekly",
  catchup=False,
  default_args=DEFAULT_ARGS,
  tags=["futebol", "brasileirao", "artilheiros"],
)
def pipeline_scorers():

  @task
  def task_extrair() -> dict:
    return extrair_dados(
      "/v4/competitions/BSA/scorers",
      {"season": "2026", "limit": "50"},
    )

  @task
  def task_validar(payload: dict) -> int:
    return validar_dados(payload, "scorers")

  @task
  def task_salvar(payload: dict) -> None:
    salvar_raw(payload, tabela="scorers_raw", chave="scorers")

  payload = task_extrair()
  validado = task_validar(payload)
  salvo = task_salvar(payload)

  validado >> salvo


pipeline_scorers()