#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

VENV_PATH=~/RADIOBLOCKS/VENV_IZARGCC
SENV_PATH=~/SKA/ska-spack-env/bipp-izar-gcc

# Activate Spack environment
source ${SENV_PATH}/activate.sh

# Activate Python virtual environment in which was installed BIPP
source ${VENV_PATH}/bin/activate

# 60 epochs
MS=/work/ska/orliac/RADIOBLOCKS/EOS_21cm_202MHz_10min_1000.MS;    LABEL=small
# 1440 epochs
#MS=/work/ska/orliac/RADIOBLOCKS/EOS_21cm-gf_202MHz_4h1d_1000.MS;  LABEL=large

TELESCOPE=SKALOW

export OMP_NUM_THREADS=1

# Mimic what happens in Bluebild to produce a reference
#
if [[ 1 == 1 ]]; then
    LOG=run_bipp_${LABEL}.log
    (time python bipp_vis.py \
        --ms $MS \
        --telescope $TELESCOPE \
        > $LOG 2>&1) &>> $LOG
fi

# Multi-processing solutions
#

#for NP in 1 2 4 8 16; do
for NP in 2 4; do
    LOG=run_mp${NP}_${LABEL}.log
    (time python multi_processing.py \
        --ms $MS \
        --telescope $TELESCOPE \
        --n_proc $NP \
        > $LOG 2>&1) &>> $LOG
done

# Deactivate Python virtual environment
deactivate 

# Deactivate Spack environment
source ${SENV_PATH}/deactivate.sh
