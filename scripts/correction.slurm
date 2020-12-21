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

input='s3a://ztf-historic-data/atesto/detections/'
format='part-{}-690cf7f6-96e3-4434-8cb3-7ebea4b2b091_{}.c000.snappy.parquet'
resources='/home/apps/astro/alercebroker/resources/'
output='/home/apps/astro/alercebroker/historic_data'
sleep `expr ${SLURM_ARRAY_TASK_ID} "*" 2`

export AWS_ACCESS_KEY_ID=AKIAQO22GL4HDJM2U64G
export AWS_SECRET_ACCESS_KEY=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

python3 get_correction.py $input $output ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format