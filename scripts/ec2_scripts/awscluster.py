#!/usr/bin/python
import sys, getopt
import boto3
import subprocess

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

def create_instances():
    print("Launching AWS Cluster")
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
            MaxCount=10,
            MinCount=1,
            SecurityGroupIds=['sg-bf13f1cf',],
            DisableApiTermination=False,
            DryRun=False,
            EbsOptimized=True,
            UserData=f.read()
        )
        print(response)

def setup_instances():
    print("Setting up instances")
    hostnames = ','.join(list_instances_attributes(['PublicDnsName',])['PublicDnsName'])
    command = "bash setup.sh {0}".format(hostnames)
    subprocess.call(command, shell=True)

def terminate_instances(instancelist):
    response = ec2client.terminate_instances(InstanceIds=instancelist)
    print(response)

def main(argv):
   try:
      opts, args = getopt.getopt(argv,"clst",["--create", "--list", "--setup", "--terminate"])
   except getopt.GetoptError:
      print 'awscluster.py -c|--create -l|--list -s|--setup -t|--terminate'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'awscluster.py -c|--create -l|--list -s|--setup -t|--terminate'
         sys.exit()
      elif opt in ("-c", "--create"):
         create_instances()
      elif opt in ("-s", "--setup"):
         setup_instances()
      elif opt in ("-l", "--list"):
         print(list_instances_attributes(['PublicDnsName',])['PublicDnsName'])
         print(list_instances_attributes(['PrivateDnsName',])['PrivateDnsName'])
      elif opt in ("-t", "--terminate"):
         instancelist = list_instances_attributes(['InstanceId',])['InstanceId']
         print(instancelist)
         terminate_instances(instancelist)

ec2client = boto3.client('ec2')
if __name__ == "__main__":
   main(sys.argv[1:])
