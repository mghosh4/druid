#!/usr/bin/python
import sys, getopt
import boto3
import subprocess
import time

SSH_OPTS = "-i ~/druid.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
EXPERIMENT = "default"

def list_instances_attributes(attributes):
    response = ec2client.describe_instances(Filters=[
        {
            "Name": "tag-value",
            "Values": [EXPERIMENT]
        }
    ])
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
        CreationToken=EXPERIMENT,
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
    response = efsclient.describe_file_systems(CreationToken=EXPERIMENT)
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    efsIP = response['MountTargets'][0]['IpAddress']

    response = efsclient.describe_file_systems(CreationToken="dependencies")
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    dependenciesEfsIP = response['MountTargets'][0]['IpAddress']

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
            InstanceType='m4.4xlarge',
            KeyName='druid',
            MaxCount=21,
            MinCount=1,
            SecurityGroupIds=['sg-bf13f1cf',],
            DisableApiTermination=False,
            DryRun=False,
            EbsOptimized=True,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'experiment',
                        'Value': EXPERIMENT
                    },
                ]
            }],
            UserData=f.read().format(efsIP=efsIP, dependenciesEfsIP=dependenciesEfsIP)
        )

def setup_instances():
    print("Setting up instances")
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    private_ips = list_instances_attributes(['PrivateIpAddress',])['PrivateIpAddress']
    private_ips.sort()

    ip_alias_lst = []
    for i, private_ip in enumerate(private_ips):
        ip_alias_lst.append("{private_ip} node-{i} node-{i}-lan node-{i}-big-lan".format(private_ip=private_ip, i=i+1))
    ip_alias = "\n".join(ip_alias_lst)
    mod_etc_hosts = "sudo bash -c 'echo \\\"{0}\\\" >> /etc/hosts'".format(ip_alias)

    private_ips = list_instances_attributes(['PrivateIpAddress',])['PrivateIpAddress']
    sorted_hostname_ip_pairs = sorted(zip(hostnames, private_ips), key=lambda x: x[1])

    for i, hostname_ip_pair in enumerate(sorted_hostname_ip_pairs):
        # command = "ssh {1} ubuntu@{0} 'bash setup.sh'".format(hostname, SSH_OPTS)
        command = "ssh {1} ubuntu@{0} \"{ip_alias}\"".format(hostname_ip_pair[0], SSH_OPTS, ip_alias=mod_etc_hosts)
        subprocess.call(command, shell=True)
        command = "ssh {1} ubuntu@{0} \"sudo bash -c 'hostnamectl set-hostname node-{i}'\"".format(hostname_ip_pair[0], SSH_OPTS, i=i+1)
        subprocess.call(command, shell=True)

def wakeup_instances():
    print("Waking up instances")
    response = efsclient.describe_file_systems(CreationToken=EXPERIMENT)
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    efsIP = response['MountTargets'][0]['IpAddress']

    response = efsclient.describe_file_systems(CreationToken="dependencies")
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    dependenciesEfsIP = response['MountTargets'][0]['IpAddress']

    mnt_efs = "sudo bash -c 'mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {fsIP}:/ {mnt_dir}'".format(fsIP=efsIP, mnt_dir="/proj")
    mnt_depfs = "sudo bash -c 'mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 {fsIP}:/ {mnt_dir}'".format(fsIP=dependenciesEfsIP, mnt_dir="/dependencies")

    mnt_ebs = "sudo bash -c 'mkfs -t ext4 /dev/xvdf && mount /dev/xvdf /druid/ && chown ubuntu:ubuntu /druid'"

    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    for hostname in hostnames:
        command = "ssh {1} ubuntu@{0} \"{mnt_cmd}\"".format(hostname, SSH_OPTS, mnt_cmd=mnt_efs)
        subprocess.call(command, shell=True)
        command = "ssh {1} ubuntu@{0} \"{mnt_cmd}\"".format(hostname, SSH_OPTS, mnt_cmd=mnt_depfs)
        subprocess.call(command, shell=True)
        command = "ssh {1} ubuntu@{0} \"{mnt_cmd}\"".format(hostname, SSH_OPTS, mnt_cmd=mnt_ebs)
        subprocess.call(command, shell=True)

def upload_artifacts():
    print("Uploading artifacts")
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    for i, hostname in enumerate(hostnames):
        # if i == 0:
        #     # command = "scp {1} ../../distribution/target/druid-0.9.0-SNAPSHOT-bin.tar.gz ubuntu@{0}:/proj/DCSQ/getafix/druid/".format(hostname, SSH_OPTS)
        #     command = "scp {1} -r ../../../druid/ ubuntu@{0}:/proj/DCSQ/getafix/druid/".format(hostname, SSH_OPTS)
        #     subprocess.call(command, shell=True)
        # command = "scp {1} setup.sh ubuntu@{0}:~/".format(hostname, SSH_OPTS)
        # subprocess.call(command, shell=True)
        command = "scp {1} druid.pem ubuntu@{0}:~/".format(hostname, SSH_OPTS)
        subprocess.call(command, shell=True)

def terminate_instances(instancelist):
    response = ec2client.terminate_instances(InstanceIds=instancelist)
    print(response)

def delete_efs():
    response = efsclient.describe_file_systems(CreationToken=EXPERIMENT)
    fileSystemId = response['FileSystems'][0]['FileSystemId']
    response = efsclient.describe_mount_targets(FileSystemId=fileSystemId)
    mountTargetId = response['MountTargets'][0]['MountTargetId']
    response = efsclient.delete_mount_target(MountTargetId=mountTargetId)
    print(response)

    response = efsclient.delete_file_system(FileSystemId=fileSystemId)
    print(response)

def getNode(idx):
    hostnames = list_instances_attributes(['PublicDnsName',])['PublicDnsName']
    private_ips = list_instances_attributes(['PrivateIpAddress',])['PrivateIpAddress']

    sorted_hostname_ip_pairs = sorted(zip(hostnames, private_ips), key=lambda x: x[1])
    if idx <= 0 or idx > len(sorted_hostname_ip_pairs):
        return None
    return sorted_hostname_ip_pairs[idx - 1][0]

def main(argv):
    try:
        opts, args = getopt.getopt(argv,"cleustdnw",["create", "efs" "list", "upload", "setup", "terminate", "delete-efs", "node", "wake"])
    except getopt.GetoptError:
        print 'awscluster.py -e|--efs -c|--create -l|--list -u|--upload -s|--setup -t|--terminate -d|--delete-efs -w|--wake <experiment namespace>'
        sys.exit(2)

    global EXPERIMENT
    EXPERIMENT = argv[1]
    for opt, arg in opts:
        if opt == '-h':
            print 'awscluster.py -e|--efs -c|--create -l|--list -u|--upload -s|--setup -t|--terminate -d|--delete-efs -w|--wake <experiment namespace>'
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
        elif opt in ("-w", "--wake"):
            wakeup_instances()
        elif opt in ("-n", "--node"):
            print getNode(int(argv[2]))

ec2client = boto3.client('ec2')
efsclient = boto3.client('efs')
if __name__ == "__main__":
    main(sys.argv[1:])
