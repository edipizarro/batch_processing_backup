#!/bin/bash
#SBATCH -J magstats
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --array=0-300%300
#SBATCH --output=/home/apps/astro/alercebroker/logs/magstats_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/magstats_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mail-type=ALL
#SBATCH --mem-per-cpu=5000

ml Python/3.7.2
source ~/venvs/env/bin/activate

input_corrected='/home/apps/astro/alercebroker/historic_data/corrected/'
input_nondet='s3a://ztf-historic-data/20201228/non_detections/'
format='part-{}-54615e63-9fa3-49ea-8077-c900ef281e26_{}.c000.snappy.parquet'
resources='/home/apps/astro/alercebroker/resources/'
output='/home/apps/astro/alercebroker/historic_data/'

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

python3 ../computing/get_magstats.py $input_corrected $input_nondet $output ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format
