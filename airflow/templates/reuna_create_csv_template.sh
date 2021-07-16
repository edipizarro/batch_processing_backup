cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
python main.py create-csv --config_json='{{params.vars}}'
{% endif %}
{% if not params.virtualenv %}
python3 main.py create-csv --config_json='{{params.vars}}'
{% endif %}
