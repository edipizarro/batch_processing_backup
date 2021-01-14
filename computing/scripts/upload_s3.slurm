#!/bin/bash
#SBATCH -J to_s3
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --output=/home/apps/astro/alercebroker/logs/to_s3_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/to_s3_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mail-type=ALL
#SBATCH --mem-per-cpu=1000

ml Python/3.7.2
source ~/venvs/env/bin/activate

input_dir=$1
output_dir=$2
pattern=$3

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

python3 ../computing/to_s3.py $input_dir $output_dir ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $pattern
