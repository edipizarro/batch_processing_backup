rm {{params.output_dir}}/*

export AWS_ACCESS_KEY_ID={{params.aws_access_key}}
export AWS_SECRET_ACCESS_KEY={{params.aws_secret_access_key}}

cd /home/apps/astro/alercebroker/batch_processing/computing/scripts

sbatch --array {{params.partitions}} {{params.script_name}} {{params.input_parquet}} {{params.input_pattern}} {{params.log_dir}} {{params.output_dir}}
