#!/bin/bash

SCRIPT_HOME=$(cd $(dirname $0) && pwd)
PYENV_HOME=$(dirname ${SCRIPT_HOME})

echo "$(date +'%Y-%m-%d %H:%M:%S')"

source /etc/profile
source /root/.bash_profile
source ${PYENV_HOME}/bin/activate

python ${PYENV_HOME}/core/jobs.py --job job_fund_scale

deactivate
