cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
{% endif %}
python main.py psql-copy-csv --config_json='{{params.vars}}' &
