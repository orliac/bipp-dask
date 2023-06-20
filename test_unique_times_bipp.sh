#!/bin/bash

set -e

export OMP_NUM_THREADS=1

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

VENV_PATH=~/RADIOBLOCKS/VENV_IZARGCC
SENV_PATH=~/SKA/ska-spack-env/bipp-izar-gcc

# Activate Spack environment
source ${SENV_PATH}/activate.sh

# Activate Python virtual environment in which was installed BIPP
source ${VENV_PATH}/bin/activate

# Check bipp installation
#python -m pip show bipp

# 60 epochs
#MS=/work/ska/orliac/RADIOBLOCKS/EOS_21cm_202MHz_10min_1000.MS;    LABEL=small
# 1440 epochs
MS=/work/ska/orliac/RADIOBLOCKS/EOS_21cm-gf_202MHz_4h1d_1000.MS;  LABEL=large

TELESCOPE=SKALOW

PY_SCRIPT=${SCRIPT_DIR}/unique_times_bipp.py

for NW in 16 8 4 2 1; do
    LOG=run_dask${NW}_${LABEL}.log
    (time python $PY_SCRIPT \
        --ms ${MS} \
        --telescope ${TELESCOPE} \
        --dask_workers ${NW} \
        --dask_tasks_per_worker 1 \
        > $LOG 2>&1) &>> ${LOG}
done

# Deactivate Python virtual environment
deactivate 

# Deactivate Spack environment
source ${SENV_PATH}/deactivate.sh
