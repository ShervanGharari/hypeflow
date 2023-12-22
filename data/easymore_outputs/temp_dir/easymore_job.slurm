#!/bin/bash
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1
#SBATCH --account=rpp-kshook
#SBATCH --time=2:00:00
#SBATCH --mem=8000M
#SBATCH --job-name=easymore
#SBATCH --error=/home/shg096/scratch/Bow_at_Calgary/easymore_outputs/temp_dir//easymore.err
#SBATCH --output=/home/shg096/scratch/Bow_at_Calgary/easymore_outputs/temp_dir//easymore.log
easymore conf /home/shg096/scratch/Bow_at_Calgary/easymore_outputs/temp_dir/easymore_conf.json