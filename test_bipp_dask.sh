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
if [[ 1 == 1 ]]; then
    python ${SCRIPT_DIR}/bipp_dask.py \
        --ms_file /home/orliac/SKA/epfl-radio-astro/bipp-bench/gauss4_t201806301100_SBL180.MS \
        --telescope LOFAR
fi

# Long example
if [[ 1 == 1 ]]; then
    python ${SCRIPT_DIR}/bipp_dask.py \
        --ms_file /work/ska/longobs_eor_skalow/EOS_21cm-gf_202MHz_4h1d_1000.MS \
        --telescope SKALOW
fi

# Deactivate Python virtual environment
deactivate 

# Deactivate Spack environment
source ${SENV_PATH}/deactivate.sh
