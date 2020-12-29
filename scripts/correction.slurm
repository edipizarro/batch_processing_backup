#!/bin/bash
#SBATCH -J correction
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --array=13,54,69,81,95,100,101,133,221,246,247,260,261,273,281
#SBATCH --output=/home/apps/astro/alercebroker/logs/correction_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/correction_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mem-per-cpu=20500

ml Python/3.7.2
source ~/venvs/env/bin/activate

input='s3a://ztf-historic-data/20201228/detections/'
format='part-{}-d27a5ff4-b27d-4ccf-9ccc-d7838431dc4b_{}.c000.snappy.parquet'
resources='/home/apps/astro/alercebroker/resources/'
output='/home/apps/astro/alercebroker/historic_data/corrected/'

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

echo "${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST}"
## sleep `expr ${SLURM_ARRAY_TASK_ID} "*" 2`

python3 ../computing/get_correction.py $input $output ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format
