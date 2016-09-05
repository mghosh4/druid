from ParseConfig import ParseConfig
import numpy as np
from scipy.integrate import simps
from numpy import trapz
import os, sys
import json
from datetime import datetime
import matplotlib.pyplot as plt

def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <config_file>"
		exit()

	configFile = getConfigFile(args)
	return configFile

def getConfigFilePath(configFile):
	return os.path.abspath("../Configs/" + configFile) 

def getConfig(configFile):
	configFilePath = configFile
	return ParseConfig(configFilePath)

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

historicalmetrics = config.getHistoricalMetric()
coordinatormetrics = config.getCoordinatorMetric()
brokermetrics = config.getBrokerMetric()
logpath = config.getLogPath()
num_h_nodes = config.getNumHistoricalNodes()
num_b_nodes = config.getNumBrokerNodes()
num_c_nodes = config.getNumCoordinatorNodes()
resultfolder = config.getResultFolder()

def RunLogReader(parameter, logfile):
		#logfile = getLogFile(sys.argv)
		#parameter = getParameter(sys.argv)
		#timerange = getTimeRange(sys.argv)

		theFile = open(logfile,'r')
		FILE = theFile.readlines()
		theFile.close()

		return_dict = dict()

		for line in FILE:
				if (parameter in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
						return_dict[y[0]['timestamp']] = y[0]['value']
		return return_dict

def Writer(dict,filename):
	f = open(filename, 'a')
	for key,value in dict.iteritems():
		f.write(key + "~" + str(value) + "\n")
	f.close()
		
def RunLogReaderWithTimeRange(parameter, logfile, timerange1, timerange2):
		#logfile = getLogFile(sys.argv)
		#parameter = getParameter(sys.argv)
		#timerange = getTimeRange(sys.argv)
		timerange = [timerange1, timerange2]
		theFile = open(logfile,'r')
		FILE = theFile.readlines()
		theFile.close()

		return_dict = dict()
		for line in FILE:
				if (parameter in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
						timestamp = datetime.strptime(y[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
						firsttimerange = datetime.strptime(timerange[0], '%Y-%m-%d:%H:%M:%S')
						secondtimerange = datetime.strptime(timerange[1], '%Y-%m-%d:%H:%M:%S')
						if(timestamp > firsttimerange):
							if(timestamp < secondtimerange):
								return_dict[y[0]['timestamp']] = y[0]['value']
							else:
								break
		return return_dict

def getMemoryUsed(parameter,logfile):
	Writer(RunLogReader(parameter, logfile), "historicalmetrics.log")

def getSegmentCount(parameter, logfile):
	Writer(RunLogReader(parameter, logfile), "coordinatormetrics.log")

def getAverageLatency():
	dictionary = dict()
	theFile = open("broker-0-query-time.log",'r')
        FILE = theFile.readlines()
        theFile.close() 

	totaltime = 0
	count = 0
	for line in FILE:
		totaltime += int(line.split("~")[1].strip())
		count += 1
	f = open("averagelatency.log", 'a')
        f.write(str(totaltime/count))
        f.close()

for i in xrange(len(historicalmetrics)):
	for j in xrange(len(num_h_nodes)):
		if "/" in historicalmetrics[i]:
			newmetrics = historicalmetrics[i].replace("/", "-")
		else:
			newmetrics = historicalmetrics[i]
		Writer(RunLogReader(historicalmetrics[i], logpath + "historical-" + str(j) + ".log"), "historical-" + str(j) + "-" + newmetrics + ".log")

for i in xrange(len(brokermetrics)):
        for j in xrange(len(num_b_nodes)):
                if "/" in brokermetrics[i]:
                        newmetrics = brokermetrics[i].replace("/", "-")
                else:
                        newmetrics = brokermetrics[i]
                Writer(RunLogReader(brokermetrics[i], logpath + "broker-" + str(j) + ".log"), "broker-" + str(j) + "-" + newmetrics + ".log")

for i in xrange(len(coordinatormetrics)):
        for j in xrange(len(num_c_nodes)):
                if "/" in coordinatormetrics[i]:
                        newmetrics = coordinatormetrics[i].replace("/", "-")
                else:
                        newmetrics = coordinatormetrics[i]
                Writer(RunLogReader(coordinatormetrics[i], logpath + "coordinator-" + str(j) + ".log"), "coordinator-" + str(j) + "-" + newmetrics + ".log")
getAverageLatency()





