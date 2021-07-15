export AWS_ACCESS_KEY_ID={{params.aws_access_key}}
export AWS_SECRET_ACCESS_KEY="{{params.aws_secret_access_key}}"
export AWS_SESSION_TOKEN="{{params.aws_session_token}}"

{% if params.rm %}
rm {{params.output_dir}}/{{params.script_name}}*.parquet
{% endif %}
cd /home/apps/astro/alercebroker/batch_processing/computing/scripts

sbatch --array {{params.partitions}} {{params.script_name}}.slurm {{params.input_parquet}} {{params.input_pattern}} {{params.log_dir}} {{params.output_dir}}
