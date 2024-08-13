#!/bin/bash

# Define variables
BASE_DIR=$1
DIST_DIR="${BASE_DIR}/dist"
VENV_PATH=$(python -m poetry run which python | sed 's|/bin/python||')
SITE_PACKAGES="${VENV_PATH}/lib/python3.9/site-packages"
SITE_PACKAGES64="${VENV_PATH}/lib64/python3.9/site-packages"

# Change to the specified directory
cd "${BASE_DIR}" || { echo "Failed to change directory to ${BASE_DIR}"; exit 1; }

# Run poetry commands
python -m poetry lock
python -m poetry install
python -m poetry build

# Get Python executable path from poetry
PYTHON_PATH=$(python -m poetry run which python)

# Install the built wheel file
${PYTHON_PATH} -m pip install "${DIST_DIR}/batch_processing-0.1.0-py3-none-any.whl"

## Zip site-packages
#cd "${SITE_PACKAGES}" || { echo "Failed to change directory to ${SITE_PACKAGES}"; exit 1; }
#zip -r "${DIST_DIR}/site-packages.zip" . || { echo "Failed to zip ${SITE_PACKAGES}"; exit 1; }

## Zip site-packages64
#cd "${SITE_PACKAGES64}" || { echo "Failed to change directory to ${SITE_PACKAGES64}"; exit 1; }
#zip -r "${DIST_DIR}/site-packages64.zip" . || { echo "Failed to zip ${SITE_PACKAGES64}"; exit 1; }

# Zip python-venv
cd "${VENV_PATH}" || { echo "Failed to change directory to ${VENV_PATH}"; exit 1; }
zip -r "${DIST_DIR}/python-venv.zip" . || { echo "Failed to zip ${VENV_PATH}"; exit 1; }

echo "Script completed successfully."
