export AWS_ACCESS_KEY_ID={{params.aws_access_key}}
export AWS_SECRET_ACCESS_KEY="{{params.aws_secret_access_key}}"
export AWS_SESSION_TOKEN="{{params.aws_session_token}}"
cd batch_processing
{% if params.virtualenv %}
conda activate {{params.virtualenv}}
python main.py create-csv --config_json='{{params.vars}}'
{% endif %}
{% if not params.virtualenv %}
python3 main.py create-csv --config_json='{{params.vars}}'
{% endif %}
