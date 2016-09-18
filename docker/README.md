# Deploy Druid Cluster with Docker

### Prerequisites:
1. Install docker and docker-machine on you development machine
1. Register a docker hub account if you don't have one, and join organization dprg. You are going to need to pull and push your docker images from there
1. Install Jinja2 using pip  
  `$ pip install jinja2`
1. For Mac OSX users only: install virtualbox

### Directories:
1. `docker/` has all the templates for docker management scripts:  
  - `build-all.sh.template`: the template for the script to build all images and push to docker hub
  - `build-conf-and-scripts.sh.template`: the template for the script to build only the image that contains conf files and getafix scripts
  - `provision.sh.template`: the template for the script to provision Emulab nodes
  - `deploy.sh.template`: the template for the script to deploy all nodes
  - `stop.sh.template`: the template for the script to stop all docker containers and remove their images from machines
1. `docker/templates/` has all the Dockerfiles templates that define images, as well as configuration templates for druid nodes, kafka, and ingestion file.
1. When running `pre_build.py`, the Dockerfile templates and conf file templates will read `config.json` and render into `docker/generated/`, the script templates will be rendered into `../`.  
  **Always remember to change template files instead of those in the `docker/generated` directory as those will be overwritten once you run `pre_build.py`**

### First Time Setup:
1. Change `docker/config.json` accordingly
1. For Mac OSX users only: Create a docker-machine dedicated for building images and setting up environment  
  `$ docker-machine create --driver virtualbox local && eval $(docker-machine env local)`
1. Prebuild  
  `$ cd docker && python pre_build.py && cd ..`
1. Run `$ ./build-all.sh` so that all the images will be built and pushed to the dprg docker hub with tag name being your username. e.g. Xiaoyao will be pushing his broker image to `dprg/druid-broker:xiaoyao`, in this way we won't be overwriting each other.

### Usage:
1. Swap in 12 nodes for your experiment. The 2 extras are for service discovery key-value store and swarm master
1. Change `docker/config.json` accordingly
1. Prebuild  
  `$ cd docker && python pre_build.py && cd ..`
1. Run `$ ./provision.sh` to provision nodes. You'll need to run this script whenever you swap in nodes.
1. Build images accordingly:  
  - If you have only changed the getafix scripts or conf files in `docker/templates/conf`, run `$ ./build-conf-and-scripts.sh`
  - Otherwise, run `$ ./build-all.sh`
1. Run `$ ./deploy.sh`

### Container Lifecycle management:
1. To see the status of all containers, run `$ eval $(docker-machine env --swarm p-druid-swarm-master) && docker ps -a`
1. To stop all containers, run `$ ./stop.sh`

### Notes:
1. To switch docker-machine, run `$ eval $(docker-machine env <the machine name>)`.   
  To switch to the swarm master, run `$ eval $(docker-machine env --swarm <the swarm master machine name>)`
1. To attach a new session to a running container, run `docker exec -it <container_name> /bin/bash`
1. To view the logs for a druid node, run `docker logs <container_name>`, or attach a new session and go to the actual log path to view the logs.
