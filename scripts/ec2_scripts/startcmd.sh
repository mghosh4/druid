#!/bin/bash

echo "Setting up the harddisk"
mkfs -t ext4 /dev/xvdf
mkdir /proj
mount /dev/xvdf /proj/
chown ubuntu:ubuntu /proj

echo "Setting up passwordless ssh"
echo 'alias ssh="ssh -i ~/druid.pem"' >> /home/ubuntu/.bash_aliases

echo "Installing packages"
apt-get update
apt-get install -y openjdk-8-jre
apt-get install -y openjdk-8-jdk
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
pip install pydruid pytz tornado pycurl

bash -c "export DEBIAN_FRONTEND=noninteractive; apt-get -q -y install mysql-server"

# NFS
mkdir /efsdata
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {efsIP}:/ /efsdata
chown ubuntu:ubuntu /efsdata
chmod go+rw /efsdata
