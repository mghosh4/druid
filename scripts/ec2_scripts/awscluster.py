#!/usr/bin/python
import sys, getopt
import boto3
import subprocess
import time

SSH_OPTS = "-i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"

def list_instances_attributes(attributes):
    response = ec2client.describe_instances()
    instancelist = dict()
    for attribute in attributes:
        instancelist[attribute] = list()

    for reservation in (response["Reservations"]):
        for instance in reservation["Instances"]:
            if instance["State"]["Name"] == "terminated":
                continue
            for attribute in attributes:
                instancelist[attribute].append(instance[attribute])

    return instancelist

def create_efs():
    print("Creating EFS")
    subnetId = 'subnet-0ed4d457'
    response = efsclient.create_file_system(
        CreationToken='druidnfs',
        PerformanceMode='generalPurpose',
        Encrypted=False
    )

    fileSystemId = response['FileSystemId']
    while True:
        response = efsclient.describe_file_systems(
            FileSystemId=fileSystemId
        )
        if response['FileSystems'][0]['LifeCycleState'] == 'available':
            break
        print('\tWaiting for EFS to settle')
        time.sleep(5)

    print("Creating EFS mount target")
    response = efsclient.create_mount_target(
        FileSystemId=fileSystemId,
        SubnetId=subnetId,
        # SecurityGroups=['sg-bf13f1cf']
    )

def create_instances():
    print("Launching EC2 Cluster")
    response = efsclient.describe_file_systems(CreationToken='druidnfs')
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    efsIP = response['MountTargets'][0]['IpAddress']

    with open('startcmd.sh') as f:
        response = ec2client.run_instances(
            BlockDeviceMappings=[
                {
                    'DeviceName': '/dev/sdf',
                    'Ebs': {
                        'Encrypted': False,
                        'DeleteOnTermination': True,
                        'VolumeSize': 64,
                        'VolumeType': 'gp2'
                    },
                },
            ],
            ImageId='ami-841f46ff',
            InstanceType='m4.large',
            KeyName='druid',
            MaxCount=10,
            MinCount=1,
            SecurityGroupIds=['sg-bf13f1cf',],
            DisableApiTermination=False,
            DryRun=False,
            EbsOptimized=True,
            UserData=f.read().format(efsIP=efsIP)
        )

def setup_instances():
    print("Setting up instances")
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    for hostname in hostnames:
        command = "ssh {1} ubuntu@{0} 'bash setup.sh'".format(hostname, SSH_OPTS)
        subprocess.call(command, shell=True)

def upload_artifacts():
    print("Uploading artifacts")
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    for hostname in hostnames:
        command = "scp {1} setup.sh ubuntu@{0}:~/".format(hostname, SSH_OPTS)
        subprocess.call(command, shell=True)
        # command = "scp {1} ../../distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz ubuntu@{0}:/proj/".format(hostname, SSH_OPTS)
        # subprocess.call(command, shell=True)

def terminate_instances(instancelist):
    response = ec2client.terminate_instances(InstanceIds=instancelist)
    print(response)

def delete_efs():
    response = efsclient.describe_file_systems(CreationToken='druidnfs')
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    mountTargetId = response['MountTargets'][0]['MountTargetId']
    response = efsclient.delete_mount_target(MountTargetId=mountTargetId)
    print(response)

    response = efsclient.delete_file_system(FileSystemId=fileSystemId)
    print(response)

def getNode(idx):
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    hostnames.sort()
    if idx <= 0 or idx > len(hostnames):
        return None
    return hostnames[idx - 1]

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"cleustdn",["create", "efs" "list", "upload", "setup", "terminate", "delete-efs", "node"])
    except getopt.GetoptError:
        print 'awscluster.py -e|--efs -c|--create -l|--list -u|--upload -s|--setup -t|--terminate -d|--delete-efs'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'awscluster.py -e|--efs -c|--create -l|--list -u|--upload -s|--setup -t|--terminate -d|--delete-efs'
            sys.exit()
        elif opt in ("-e", "--efs"):
            create_efs()
        elif opt in ("-c", "--create"):
            create_instances()
        elif opt in ("-s", "--setup"):
            setup_instances()
        elif opt in ("-u", "--upload"):
            upload_artifacts()
        elif opt in ("-l", "--list"):
            print(list_instances_attributes(['PublicDnsName',])['PublicDnsName'])
            print(list_instances_attributes(['PrivateDnsName',])['PrivateDnsName'])
        elif opt in ("-t", "--terminate"):
            instancelist = list_instances_attributes(['InstanceId',])['InstanceId']
            print(instancelist)
            terminate_instances(instancelist)
        elif opt in ("-d", "--delete-efs"):
            delete_efs()
        elif opt in ("-n", "--node"):
            print getNode(int(argv[1]))

ec2client = boto3.client('ec2')
efsclient = boto3.client('efs')
if __name__ == "__main__":
    main(sys.argv[1:])
