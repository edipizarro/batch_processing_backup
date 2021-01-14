# Batch Processing
Repository for all related to batch processing of historic data


## Compute in Leftraru

```bash
sbatch --array [0-n]%n+1 magstats.slurm {corrected_path} {non_det_path} {format_non_det}
```