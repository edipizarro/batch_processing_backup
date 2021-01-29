from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults


class SSHCommandSensor(BaseSensorOperator, SSHOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def poke(self, context):
        try:
            SSHOperator.execute(self, context)
            return True
        except AirflowException:
            return False
