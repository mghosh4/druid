#!/usr/bin/env python2.7

import csv
import argparse
import json
import random
import subprocess
import time
from datetime import datetime, timedelta
import os
import sys

from ParseIngestionConfig import ParseIngestionConfig

# Argument parsing
parser = argparse.ArgumentParser()
#parser.add_argument('-n', metavar='count', type=int, default=1)
parser.add_argument('file', type=argparse.FileType('r'))
args = parser.parse_args()

def getConfigFile(args):
  return args[1]

def checkAndReturnArgs(args):
  requiredNumOfArgs = 1
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
delimiter = config.getDelimiter()
#index_task = config.getIndexTask()
#overlord_host = config.getOverlordHost()
kafkahost = config.getKafkaHost()
runtime = config.getRunTime()
os.chdir(kafkapath)

# Open the kafka console producer
producer = subprocess.Popen(
  "./bin/kafka-console-producer.sh --broker-list %s:9092 --topic %s" %(kafkahost, kafkatopic),
  shell=True,
  stdin=subprocess.PIPE
)

# Generate random query metrics
if(datatype == "randomstream"):
  endtime = datetime.now() + timedelta(minutes=runtime)
  while True:
    if datetime.now() >= endtime:
      break
    metric = {
      'timestamp': long(time.time() * 1000),
      'name': 'query/time',
      'host': '192.168.1.' + str(random.randrange(1, 50)),
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

elif(datatype == "customstream"):

  r = csv.reader(open(datafilepath,'rU'), dialect=csv.excel_tab)
  header = next(r)
#  print header
  metrics = []
  line = header[0].split(delimiter)
  for word in line:
    metrics.append(word)
  csvreader = csv.reader(open(datafilepath,'rU'), dialect=csv.excel_tab)
  next(csvreader)
  count = 0
  for rows in csvreader:
    line2 = rows[0].split(delimiter)
    metrics_dict = {}
    metricsvalues = []
    for word in line2:
	if(is_number(word)):
		number = float(word)
		metricsvalues.append(number)
	else:
        	metricsvalues.append(word)
    for i in xrange(len(metrics)):
	metrics_dict[metrics[i]] = metricsvalues[i]
    if count == 0:
      print json.dumps(metrics_dict)
    producer.stdin.write(json.dumps(metrics_dict))
    producer.stdin.write("\n")
    count += 1
  producer.stdin.close()
  producer.wait()

  if producer.returncode != 0:
    raise Exception("Producer exited with code: " + str(producer.returncode))

elif(datatype == "batch"):
  os.system("curl -X 'POST' -H 'Content-Type:application/json' -d %s.json %s:8090/druid/indexer/v1/task" % (index_task, overlord_host)) 
 # f = open(datafilepath, 'r')
 # first = f.readline()
  #split line into metric parts
 # metrics = []
 # for word in first.split(delimiter):
 #   print word
 #   metrics.append(word)
  #then feed each subsequent line into the producer
 # for line in f:
 #   if(line != f):
 #     metrics_dict = {}
 #     metricsvalues = []
 #     for word in line.split(delimiter):
 #       metricsvalues.append(word)
 #     for i in xrange(len(metrics)):
 #       metrics_dict[metrics[i]] = metricsvalues[i]
 #     producer.stdin.write(json.dumps(metrics_dict))
 #     producer.stdin.write("\n")

  # Close kafka console producer, wait for exit
 # producer.stdin.close()
 # producer.wait()

 # if producer.returncode != 0:
 #   raise Exception("Producer exited with code: " + str(producer.returncode))  




