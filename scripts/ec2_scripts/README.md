When bootstrapping a new experiment:
1. Create EFS: `python awscluster.py -e <experiment_namespace>`
1. Create S3: `python awscluster.py -g <experiment_namespace>`
1. Create EC2: `python awscluster.py -c <experiment_namespace>`
1. Upload druid.pem: `python awscluster.py -u <experiment_namespace>`
1. Setup hostname resolutions: `python awscluster.py -s <experiment_namespace>`
1. Setup druid artifacts by: git clone druid, mvn install, untar built tar
1. Modify scripts/deployment/config/getafix.aws.conf to reflect AWS keys, and bucket name
1. start_druid.sh
1. run.sh

When EC2s are restarted:
1. Go to AWS dashboard and remove bucket
1. Recreate S3 to ensure clean deep storage: `python awscluster.py -g <experiment_namespace>`
1. Wake up and remount EFS and EBS: `python awscluster.py -w <experiment_namespace>`
