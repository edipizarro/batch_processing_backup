{% if params.virtualenv %}
conda activate {{params.virtualenv}}
{% endif %}
export AWS_ACCESS_KEY_ID={{params.aws_access_key}}
export AWS_SECRET_ACCESS_KEY={{params.aws_secret_access_key}}
cd batch_processing
python main.py create-csv --config_json='{{params.vars}}'
