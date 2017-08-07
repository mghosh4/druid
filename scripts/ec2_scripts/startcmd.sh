#!/bin/sh

sudo mkfs -t ext4 /dev/xvdf
sudo mkdir /druid
sudo mount /dev/xvdf /newvolume/

sudo apt-get update
sudo dpkg --configure -a
# Some useful utilities
sudo apt-get install -y openjdk-7-jre
sudo apt-get install -y openjdk-7-jdk
sudo apt-get install -y curl
sudo apt-get install -y screen
sudo apt-get install -y wget
sudo apt-get install -y vim
sudo apt-get install -y gnuplot
sudo apt-get install -y python-numpy
sudo apt-get install -y maven
sudo apt-get install -y python-pip
sudo apt-get install -y python-pandas
sudo apt-get install -y libcurl4-openssl-dev
sudo pip install pydruid pytz tornado pycurl

bash -c "export DEBIAN_FRONTEND=noninteractive; sudo -E apt-get -q -y install mysql-server"
