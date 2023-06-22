#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

VENV_PATH=~/RADIOBLOCKS/VENV_IZARGCC
SENV_PATH=~/SKA/ska-spack-env/bipp-izar-gcc

# Activate Spack environment
source ${SENV_PATH}/activate.sh

# Activate Python virtual environment in which was installed BIPP
source ${VENV_PATH}/bin/activate


# Short example
if [[ 1 == 0 ]]; then
    python ${SCRIPT_DIR}/bipp_dask.py \
        --ms_file /home/orliac/SKA/epfl-radio-astro/bipp-bench/gauss4_t201806301100_SBL180.MS \
        --telescope LOFAR \
        --bipp_vis
fi

# Medium example
if [[ 1 == 0 ]]; then
    CL_RUN="python ${SCRIPT_DIR}/bipp_dask.py \
        --ms_file /work/ska/orliac/RADIOBLOCKS/EOS_21cm_202MHz_10min_1000.MS \
        --telescope SKALOW \
        --n_epochs 0"
    echo "------------------------------------------"
    echo $CL_RUN
    echo "------------------------------------------"
    if [[ $RUN_VTUNE == 1 ]]; then
        source /work/ska/soft/intel/jed/vtune/latest/vtune-vars.sh
        which vtune
        vtune -collect=hotspots -run-pass-thru=--no-altstack -strategy ldconfig:notrace:notrace -- \
            $CL_RUN
    else
        $CL_RUN
    fi
fi

# Long example
if [[ 1 == 0 ]]; then
    python ${SCRIPT_DIR}/bipp_dask.py \
        --ms_file /work/ska/orliac/RADIOBLOCKS/EOS_21cm-gf_202MHz_4h1d_1000.MS \
        --telescope SKALOW \
        --bipp_vis \
        --n_epochs 100
fi

# Deactivate Python virtual environment
deactivate 

# Deactivate Spack environment
source ${SENV_PATH}/deactivate.sh
