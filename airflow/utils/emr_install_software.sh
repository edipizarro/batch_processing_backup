#!/bin/bash

sudo yum install -y git
git clone https://github.com/alercebroker/batch_processing.git /tmp/batch_processing
sudo python3 -m pip install -r /tmp/batch_processing/requirements.txt
sudo python3 -m pip install ipython
echo "export PYSPARK_DRIVER_PYTHON=ipython3" >> $HOME/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3.6" >> $HOME/.bashrc
source $HOME/.bashrc
