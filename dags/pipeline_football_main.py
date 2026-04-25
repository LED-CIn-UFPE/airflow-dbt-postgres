from datetime import datetime, timedelta
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag
def pipeline_futebol():

    @task
    def extrair_partidas() -> dict:
      pass

    @task
    def validar_partidas(payload: dict) -> int:
      pass

    @task
    def salvar_partidas(payload: dict) -> None:
      pass

    payload = extrair_partidas()
    validado = validar_partidas(payload)
    salvo = salvar_partidas(payload)

    validado >> salvo


pipeline_futebol()