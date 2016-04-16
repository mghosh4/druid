#!/usr/bin/env python2.7

import argparse
import json
import random
import subprocess
import time

from ParseIngestionConfig import ParseIngestionConfig

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('-n', metavar='count', type=int, default=1)
parser.add_argument('file', type=argparse.FileType('r'))
args = parser.parse_args()

def getConfigFile(args):
  return args[3]

def checkAndReturnArgs(args):
  requiredNumOfArgs = 2
  if len(args) < requiredNumOfArgs:
    print "Usage: python " + args[0] + " <config_file>"
    exit()

  configFile = getConfigFile(args)
  return configFile

def getConfigFilePath(configFile):
  return os.path.abspath(configFile) 

def getConfig(configFile):
  configFilePath = configFile
  return ParseIngestionConfig(configFilePath)

def is_number(s):
  try:
    float(s)
    return True
  except ValueError:
    return False

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)
kafkapath = config.getKafkaPath()
kafkatopic = config.getKafkaTopic()
datafilepath = config.getDataFilePath()
datatype = config.getDataType()

os.chdir(kafkapath)

# Open the kafka console producer
producer = subprocess.Popen(
  "./bin/kafka-console-producer.sh --broker-list node-18-big-lan:9092 --topic %s" %(kafkatopic),
  shell=True,
  stdin=subprocess.PIPE
)

# Generate random query metrics
if(datatype == "random"):
  for n in xrange(0, args.n):
    metric = {
      'timestamp': long(time.time() * 1000),
      'name': 'query/time',
      'host': '192.168.' + str(random.randrange(1, 254)) + '.' + str(random.randrange(1, 254)),
      'page': str(int(max(1, random.gauss(5, 4)))) + '.html',
      'value': max(0, int(random.gauss(200, 80)))
    }
    producer.stdin.write(json.dumps(metric))
    producer.stdin.write("\n")

  # Close kafka console producer, wait for exit
  producer.stdin.close()
  producer.wait()

  if producer.returncode != 0:
    raise Exception("Producer exited with code: " + str(producer.returncode))

elif(datatype == "custom"):
  f = open(datafilepath, 'r')
  first = f.readline()
  #split line into metric parts
  metrics = []
  for word in first.split(delimiter):
    metrics.append(word)
  #then feed each subsequent line into the producer
  for line in f:
    if(line != first):
      metrics_dict = {}
      metricsvalues = []
      for word in line.split(delimiter):
        if(is_number(word)):
        {
          number = float(word)
          metricsvalues.append(number)
        }
        else:
        {
          metricsvalues.append(word)
        }
      for i in xrange(len(metrics)):
        metrics_dict[metrics[i]] = metricsvalues[i]
      producer.stdin.write(json.dumps([{metrics[i]: metricsvalues[i],} for i in xrange(len(metrics))]))
      #producer.stdin.write(json.dumps(metrics_dict))
      producer.stdin.write("\n")

  # Close kafka console producer, wait for exit
  producer.stdin.close()
  producer.wait()

  if producer.returncode != 0:
    raise Exception("Producer exited with code: " + str(producer.returncode))  



