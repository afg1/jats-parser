#!/bin/bash
#SBATCH --job-name=convert_xml_2_parquet
#SBATCH --array=0-981
#SBATCH -p datamover
#SBATCH --mem 4G
#SBATCH --time=8:00:00


cd /hps/nobackup/agb/rnacentral/agreen/ARISE/jats-parser/

poetry run python xml_2_parquet.py /nfs/ftp/public/databases/pmc/oa/ /hps/nobackup/agb/rnacentral/agreen/ARISE/jats-parser/oa_parquet --file_index $SLURM_ARRAY_TASK_ID