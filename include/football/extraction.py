import logging
from airflow.providers.http.hooks.http import HttpHook

logger = logging.getLogger(__name__)


def extrair_dados(endpoint: str, params: dict) -> dict:
  hook = HttpHook(method="GET", http_conn_id="football_data_api")
  response = hook.run(
    endpoint=endpoint,
    data=params,
    headers={"Content-Type": "application/json"},
  )
  return response.json()


def validar_dados(payload: dict, chave: str) -> int:
  dados = payload.get(chave, [])

  if not dados:
    raise ValueError(f"Nenhum dado retornado para a chave '{chave}'.")

  logger.info(f"Total de registros ({chave}): {len(dados)}")
  return len(dados)