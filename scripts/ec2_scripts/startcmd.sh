#!/bin/bash

echo "Setting up the harddisk"
mkfs -t ext4 /dev/xvdf
mkdir /druid
mount /dev/xvdf /druid/
chown -R ubuntu:ubuntu /druid

echo "Setting up passwordless ssh"
echo 'alias ssh="ssh -i ~/druid.pem"' >> /home/ubuntu/.bash_aliases

echo "Installing packages"
apt-get update
apt-get install -y openjdk-7-jre
apt-get install -y openjdk-7-jdk
apt-get install -y curl
apt-get install -y screen
apt-get install -y wget
apt-get install -y vim
apt-get install -y pdsh
apt-get install -y gnuplot
apt-get install -y python-numpy
apt-get install -y maven
apt-get install -y python-pip
apt-get install -y python-pandas
apt-get install -y libcurl4-openssl-dev
apt-get install -y nfs-common
apt-get install -y git
pip install pydruid pytz tornado pycurl

bash -c "export DEBIAN_FRONTEND=noninteractive; apt-get -q -y install mysql-server"

# NFS
mkdir /proj
mkdir /dependencies

mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {efsIP}:/ /proj
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {dependenciesEfsIP}:/ /dependencies

PATH_TO_DRUID=/proj/DCSQ/getafix/druid
PATH_TO_TARFILES=/proj/DCSQ/getafix/tarfiles
PATH_TO_DEPS=/proj/DCSQ/getafix/dependencies

mkdir -p $PATH_TO_DRUID
mkdir -p $PATH_TO_TARFILES

ln -s /dependencies $PATH_TO_DEPS

chown -R ubuntu:ubuntu /proj
chown -R ubuntu:ubuntu /dependencies
chmod go+rw /proj
chmod go+rw /dependencies
