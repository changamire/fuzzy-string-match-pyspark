#!/bin/bash

VIRN_ENV_NAME=.venv

SETUP=$1

set -e

if [ "${SETUP}" = "setup" ]; then
  virtualenv "${VIRN_ENV_NAME}" -p python3
fi

source "${VIRN_ENV_NAME}"/bin/activate

if [ "${SETUP}" = "setup" ]; then
  pip3 install 'pip-tools==3.1.0'
  pip3 install pytest

  pip-compile --output-file requirements.txt requirements.in
  pip install -r requirements.txt
fi

pytest -v -s