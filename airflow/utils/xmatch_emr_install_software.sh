#!/usr/bin/env bash
set -e

# Install git
sudo yum install -y git
# Clone and install required packages for batch processing
git clone --branch feature/xmatch_airflow https://github.com/alercebroker/batch_processing.git /tmp/batch_processing
sudo python3 -m pip install -r /tmp/batch_processing/requirements.txt
sudo python3 -m pip install ipython
# Add pyspark env variables
echo "export PYSPARK_DRIVER_PYTHON=ipython3" >> $HOME/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3.6" >> $HOME/.bashrc
source $HOME/.bashrc
# Clone and install required packages for astroide
git clone https://github.com/alercebroker/minimal_astroide.git /tmp/minimal_astroide
## Install gradle
wget https://services.gradle.org/distributions/gradle-4.4.1-bin.zip -O /tmp/gradle.zip
sudo unzip -d /opt /tmp/gradle.zip
ls /opt
sudo ln -s /opt/gradle-4.4.1 /opt/gradle
export GRADLE_HOME=/opt/gradle
export PATH=$PATH:/opt/gradle/bin
gradle -v
cd /tmp/minimal_astroide
gradle build
cp /tmp/minimal_astroide/build/libs/minimal_astroide.jar /tmp
cp /tmp/minimal_astroide/libs/healpix-1.0.jar /tmp
sudo python3 /tmp/minimal_astroide/python_wrapper/setup.py install

