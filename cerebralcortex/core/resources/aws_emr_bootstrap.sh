#!/bin/bash
set -e
# update pythong to 3.6
sudo yum update -y
sudo yum install -y python36
sudo yum install -y python36-devel

# install git
sudo yum install -y git

# Install pythong packages - CerebralCortex
sudo pip-3.6 install --egg mysql-connector-python-rf
sudo pip-3.6 install wheel==0.29.0
sudo pip-3.6 install pytz==2017.2
sudo pip-3.6 install PyYAML==3.12
sudo pip-3.6 install minio==2.2.4
sudo pip-3.6 install kafka==1.3.5
sudo pip-3.6 install influxdb==5.0.0
sudo pip-3.6 install pyarrow==0.8.0
sudo pip-3.6 install pympler==0.5
sudo pip-3.6 install hdfs3==0.3.0

# Install pythong packages - CerebralCortex-DataAnalysis
sudo pip-3.6 install pandas
sudo pip-3.6 install geopy
sudo pip-3.6 install scikit-learn
sudo pip-3.6 install numpy==1.14.2
sudo pip-3.6 install scipy
sudo pip-3.6 install pympler
sudo pip-3.6 install shapely
sudo pip-3.6 install bs4
sudo pip-3.6 install sphinx
sudo pip-3.6 install python-google-places
sudo pip-3.6 install keras
sudo pip-3.6 install tensorflow
sudo pip-3.6 install bs4

# copy cc config file to home dir
aws s3 cp s3://cerebralcortex-resources/cc_config/cc_aws_config.yml ~/
aws s3 cp s3://cerebralcortex-resources/scripts/prepare_cc.sh ~/