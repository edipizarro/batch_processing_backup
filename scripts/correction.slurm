#!/bin/bash
#SBATCH -J correction
#SBATCH -p general
#SBATCH -n 1
#SBATCH --array=1
#SBATCH --output=/home/apps/astro/alercebroker/logs/correction_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/correction_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mail-type=ALL
#SBATCH --mem-per-cpu=15000

ml Python/3.7.2
source ~/venvs/env/bin/activate

input='s3://ztf-historic-data/airflowtest/detections/'
format='part-{}-37ce8946-4e58-4ded-ac4d-543492b6e405_{}.c000.snappy.parquet'
resources='/home/apps/astro/alercebroker/resources/'
output='/home/apps/astro/alercebroker/historic_data/corrected/'
sleep `expr ${SLURM_ARRAY_TASK_ID} "*" 2`

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

python3 ../computing/get_correction.py $input $output ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format
