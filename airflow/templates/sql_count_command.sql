{% set xcom=ti.xcom_pull(task_ids=params.task_name) %}

select count(*) from {{params.table}}
having count(*) = {% if xcom -%} {{xcom.strip().decode()}} {% endif %}
