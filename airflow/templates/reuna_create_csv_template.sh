cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
{% endif %}
python  main.py create-csv --config_json='{{params.vars}}'
