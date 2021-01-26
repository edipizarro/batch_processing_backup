{% set xcom=ti.xcom_pull(task_ids='launch_slurm_script_for_{{params.table}}') %}
cd /home/apps/astro/alercebroker/batch_processing/computing/scripts
bash check_finished.sh {% if xcom %} {{''.join([x for x in xcom.strip().decode() if str.isdigit(x)])}} {% endif %}
