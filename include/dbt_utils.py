import os
import logging
import subprocess

logger = logging.getLogger(__name__)

DBT_DIR = "/usr/local/airflow/include/dbt"

DBT_ENV = {
  **os.environ,
  "DBT_TARGET_PATH": "/tmp/dbt/target",
  "DBT_LOG_PATH": "/tmp/dbt/logs",
}


def dbt_deps() -> None:
  cmd = ["dbt", "deps", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]
  _executar(cmd)


def dbt_run(select: str = None, exclude: str = None) -> None:
  cmd = ["dbt", "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]
  if select:
    cmd += ["--select", select]
  if exclude:
    cmd += ["--exclude", exclude]
  _executar(cmd)


def dbt_test(select: str = None) -> None:
  cmd = ["dbt", "test", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]
  if select:
    cmd += ["--select", select]
  _executar(cmd)


def dbt_build(select: str = None) -> None:
  cmd = ["dbt", "build", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR]
  if select:
    cmd += ["--select", select]
  _executar(cmd)


def _executar(cmd: list[str]) -> None:
  logger.info(f"Rodando: {' '.join(cmd)}")
  result = subprocess.run(
    cmd,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    env=DBT_ENV,
  )
  if result.stdout:
    logger.info(result.stdout)
  if result.returncode != 0:
    raise RuntimeError(
      f"dbt falhou com exit code {result.returncode}\n{result.stdout}"
    )