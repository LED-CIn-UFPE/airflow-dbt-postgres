from datetime import timedelta
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState


def wait_for_dag(dag_id: str, timeout: int = 3600) -> ExternalTaskSensor:
  return ExternalTaskSensor(
    task_id=f"wait_{dag_id}",
    external_dag_id=dag_id,
    allowed_states=[DagRunState.SUCCESS],
    failed_states=[DagRunState.FAILED],
    timeout=timeout,
    poke_interval=60,
  )