#!/bin/sh

#SBATCH -J features
#SBATCH -p slims
#SBATCH -n 1
#SBATCH --output=/home/apps/astro/alercebroker/logs/features_%A_%a.txt
#SBATCH --error=/home/apps/astro/alercebroker/logs/features_%A_%a.err
#SBATCH --mail-user=javier.arredondo.c@usach.cl
#SBATCH --mem-per-cpu=17828

ml Python/3.7.2
source ~/venvs/env/bin/activate

input_corrected=$1
input_objects=$2
input_nondet=$3
format=$4
resources='/home/apps/astro/alercebroker/resources/'
output='/home/apps/astro/alercebroker/historic_data/'

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1

python3 ../computing/_features.py $input_corrected $input_objects $input_nondet $output ${SLURM_ARRAY_TASK_ID} ${SLURM_JOB_NODELIST} ${SLURM_ARRAY_JOB_ID} $resources --file-format $format
