#!/bin/bash
#SBATCH -J correction
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --output=/home/apps/astro/alercebroker/logs/correction_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/correction_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mem-per-cpu=20500

ml Python/3.7.2
source ~/venvs/env/bin/activate

input=$1
format=$2
resources=$3
output=$4

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

echo "${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST}"

python3 ../computing/get_correction.py ${input} ${output} ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format
