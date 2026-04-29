import json
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def salvar_raw(payload: dict, tabela: str, chave: str) -> None:
  hook = PostgresHook(postgres_conn_id="postgres_football")
  conn = hook.get_conn()
  cursor = conn.cursor()

  cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
  cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS raw.{tabela} (
      id          SERIAL PRIMARY KEY,
      payload     JSONB,
      ingested_at TIMESTAMP DEFAULT NOW()
    );
  """)

  cursor.execute(
    f"INSERT INTO raw.{tabela} (payload) VALUES (%s);",
    (json.dumps(payload),)
  )

  conn.commit()
  cursor.close()
  conn.close()

  logger.info(f"Payload salvo em raw.{tabela} com {len(payload.get(chave, []))} registros.")