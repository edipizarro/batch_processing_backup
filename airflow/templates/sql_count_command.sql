{% set xcom=ti.xcom_pull(task_ids='count_csv_tuples_{{params.table}}') %}

select count(*) from {{params.table}}
having count(*) = {% if xcom -%} {{xcom.strip().decode()}} {% endif %}
