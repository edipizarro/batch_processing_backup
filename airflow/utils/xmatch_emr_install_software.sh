#!/usr/bin/env bash
set -euo pipefail

# Install git
sudo yum install -y git
# Clone and install required packages for batch processing
git clone https://github.com/alercebroker/batch_processing.git /tmp/batch_processing
sudo python3 -m pip install -r /tmp/batch_processing/requirements.txt
sudo python3 -m pip install ipython
# Add pyspark env variables
echo "export PYSPARK_DRIVER_PYTHON=ipython3" >> $HOME/.bashrc
echo "export PYSPARK_PYTHON=/usr/bin/python3.6" >> $HOME/.bashrc
# Clone and install required packages for astroide
git clone https://github.com/alercebroker/minimal_astroide.git /tmp/minimal_astroide
cd /tmp/minimal_astroide
## Install gradle
echo "Wget gradle"
wget https://services.gradle.org/distributions/gradle-4.4.1-bin.zip -O /tmp/gradle.zip
echo "Unzip gradle"
sudo unzip -d /opt /tmp/gradle.zip
echo "Unzipped /tmp/gradle.zip to /opt"
ls /opt
sudo ln -s /opt/gradle-4.4.1 /opt/gradle
echo "export GRADLE_HOME=/opt/gradle" >> $HOME/.bashrc
echo "export PATH=\$PATH:/opt/gradle/bin" >> $HOME/.bashrc
source $HOME/.bashrc
gradle -v
gradle build
cp build/minimal_astroide.jar /tmp
cp libs/healpix-1.0.jar /tmp
sudo python3 /tmp/minimal_astroide/python_wrapper/setup.py install

