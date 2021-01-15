{% if params.virtualenv %}
conda activate {{params.virtualenv}}
{% endif %}
cd batch_processing
python main.py create-csv --config_json='{{params.vars}}'
