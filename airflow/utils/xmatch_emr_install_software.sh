#!/usr/bin/env bash
set -euo pipefail

sudo yum install -y git
git clone https://github.com/alercebroker/batch_processing.git /tmp/batch_processing
sudo python3 -m pip install -r /tmp/batch_processing/requirements.txt
sudo python3 -m pip install ipython
git clone https://github.com/alercebroker/minimal_astroide.git /tmp/minimal_astroide
cd /tmp/minimal_astroide
gradle build
cp build/minimal_astroide.jar /tmp
cp libs/healpix-1.0.jar /tmp
sudo python3 /tmp/minimal_astroide/python_wrapper/setup.py install
echo "export PYSPARK_DRIVER_PYTHON=ipython3" >> $HOME/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3.6" >> $HOME/.bashrc
source $HOME/.bashrc

