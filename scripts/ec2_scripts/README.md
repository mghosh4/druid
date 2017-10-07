When bootstrapping a new experiment:
1. Create EFS: `python awscluster.py -e <namespace>`
1. Create S3: `python awscluster.py -g <namespace>`
1. Create EC2: `python awscluster.py -c <namespace> <num_nodes>`
1. Upload druid.pem: `python awscluster.py -u <namespace>`
1. Setup hostname resolutions: `python awscluster.py -s <namespace>`
1. To ssh into a node: ssh -i ~/druid.pem ubuntu@`python awscluster.py -n <namespace> <node_number>`
1. Setup druid artifacts by: git clone druid, mvn install, untar built tar.
1. Modify scripts/deployment/config/getafix.aws.conf to reflect AWS keys, and bucket name
1. start_druid.sh
1. run.sh

When EC2s are restarted:
1. Go to AWS dashboard and remove S3 bucket
1. Recreate S3 to ensure clean deep storage: `python awscluster.py -g <namespace>`
1. Wake up and remount EFS and EBS: `python awscluster.py -w <namespace>`
