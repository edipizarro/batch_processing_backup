{% set xcom=ti.xcom_pull(task_ids=params.task_name) %}

cd /home/apps/astro/alercebroker/batch_processing/computing/scripts
JOBID=$(echo "{{xcom.strip().decode()}}" | tr -dc '0-9')
bash check_finished.sh $JOBID
