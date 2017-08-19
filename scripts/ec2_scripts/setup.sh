#!/bin/bash
runCommand(){
    echo "Running Command:" $1
    $1
}

PATH_TO_DRUID=/proj/DCSQ/mghosh4/druid
PATH_TO_DEPS=/proj/DCSQ/mghosh4/dependencies
PATH_TO_TARFILES=/proj/DCSQ/mghosh4/tarfiles # Make this folder outside druid folder to be safe

echo "Creating the tar"
mkdir -p $PATH_TO_TARFILES/druid
cd $PATH_TO_TARFILES/druid
if [ -d distribution ]; then
    echo "Removing Druid Binary"
    sudo rm -rf distribution
    sudo rm -rf druid-0.9.0-SNAPSHOT
fi

if [ -d scripts ]; then
    echo "Removing Scripts"
    sudo rm -rf scripts
fi

if [ ! -d dependencies ]; then
    echo "Copying Dependencies"
    cp -r $PATH_TO_DEPS .
fi

echo "Copying Druid Binary"
cp -r $PATH_TO_DRUID/distribution .
tar -xf distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz
echo "Copying Scripts"
cp -r $PATH_TO_DRUID/scripts .

cd ..
tar -cf druid.tar.gz -C druid/ .

cd $PATH_TO_DRUID/scripts/ec2_scripts
echo "Sending the tar file to all nodes"
runCommand "pdsh -R exec -f 5 -w $1 -l ubuntu scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i druid.pem -o ConnectTimeout=5 $PATH_TO_TARFILES/druid.tar.gz druid.pem %u@%h:."
#echo "Sending the private key"
#runCommand "pdsh -R exec -f 5 -w $1 -l ubuntu scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i druid.pem -o ConnectTimeout=5 druid.pem %u@%h:~/"

echo "Untarring the file in all nodes"
runCommand "pdsh -R exec -f 5 -w $1 -l ubuntu ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i druid.pem -o ConnectTimeout=5 %u@%h 'cd /druid/; sudo tar -xf /home/ubuntu/druid.tar.gz; sudo mkdir logs'"
