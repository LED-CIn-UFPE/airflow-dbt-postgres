import logging

from airflow.providers.http.hooks.http import HttpHook

logger = logging.getLogger(__name__)


def extrair_partidas() -> dict:
  hook = HttpHook(method="GET", http_conn_id="football_data_api")

  response = hook.run(
    endpoint="/v4/competitions/BSA/matches",
    data={"season": "2026"},
    headers={"Content-Type": "application/json"},
  )

  return response.json()


def validar_partidas(payload: dict) -> int:
  partidas = payload.get("matches", [])

  if not partidas:
    raise ValueError("Nenhuma partida retornada pela API.")

  finalizadas = sum(1 for p in partidas if p.get("status") == "FINISHED")
  agendadas = sum(1 for p in partidas if p.get("status") == "SCHEDULED")

  logger.info(f"Total de partidas: {len(partidas)}")
  logger.info(f"  → Finalizadas : {finalizadas}")
  logger.info(f"  → Agendadas   : {agendadas}")

  return len(partidas)