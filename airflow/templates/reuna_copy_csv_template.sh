cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
python main.py psql-copy-csv --config_json='{{params.vars}}'
{% endif %}
{% if not params.virtualenv %}
python3 main.py psql-copy-csv --config_json='{{params.vars}}'
{% endif %}
