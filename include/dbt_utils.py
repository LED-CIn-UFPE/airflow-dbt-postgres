import subprocess
import logging

logger = logging.getLogger(__name__)

DBT_DIR = "/usr/local/airflow/include/dbt"


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
  result = subprocess.run(cmd, capture_output=True, text=True)
  if result.stdout:
    logger.info(result.stdout)
  if result.returncode != 0:
    logger.error(result.stderr)
    raise RuntimeError(f"dbt falhou com exit code {result.returncode}")