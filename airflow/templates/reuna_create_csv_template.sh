cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
{% endif %}
nohup python  main.py create-csv --config_json='{{params.vars}}' &
