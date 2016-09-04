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

parameterforbroker = config.getParameterForBroker()
parameterforhistorical = config.getParameterForHistorical()
parameterforcoordinator = config.getParameterForCoordinator()
pathforcoordinator = config.getPathForCoordinator()
pathforhistorical = config.getPathForHistorical()
pathforbroker = config.getPathForBroker()
timerangeforhistorical = config.getTimeRangeForHistorical()
timerangeforbroker = config.getTimeRangeForBroker()

def RunLogReader(parameter, logfile, filename):
		#logfile = getLogFile(sys.argv)
		#parameter = getParameter(sys.argv)
		#timerange = getTimeRange(sys.argv)

		theFile = open(logfile,'r')
		FILE = theFile.readlines()
		theFile.close()

		f = open(filename, 'a')
		for line in FILE:
				if (parameter in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
						f.write(y[0]['timestamp'] + "~" + str(y[0]['value']) + "\n")
		f.close()
def RunLogReaderWithTimeRange(parameter, logfile, filename, timerange1, timerange2):
		#logfile = getLogFile(sys.argv)
		#parameter = getParameter(sys.argv)
		#timerange = getTimeRange(sys.argv)
		timerange = [timerange1, timerange2]
		theFile = open(logfile,'r')
		FILE = theFile.readlines()
		theFile.close()

		f = open(filename, 'a')
		for line in FILE:
				if (parameter in line):
						eventindex = line.find("Event")
						event = line[eventindex+6:]
						y = json.loads(event)
						timestamp = datetime.strptime(y[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
						firsttimerange = datetime.strptime(timerange[0], '%Y-%m-%d:%H:%M:%S')
						secondtimerange = datetime.strptime(timerange[1], '%Y-%m-%d:%H:%M:%S')
						if(timestamp > firsttimerange and timestamp < secondtimerange):
										f.write(y[0]['timestamp'] + "~" + str(y[0]['value']) + "\n")
		f.close()

def getMemoryUsed(parameter,logfile):
	RunLogReader(parameter, logfile, "historicalmetrics.log")

def getSegmentCount(parameter, logfile):
	RunLogReader(parameter, logfile, "coordinatormetrics.log")

def getAverageLatency(parameter, logfile):
	RunLogReader(parameter, logfile, "averagelatency.log")
def plotMemoryUsed():
	plt.plotfile('historicalmetrics.log', delimiter='~', cols=(0, 1), 
			 names=('times', 'values'))
	plt.show()

def plotSegmentCount():
	plt.plotfile('coordinatormetrics.log', delimiter='~', cols=(0, 1), 
			 names=('times', 'values'))
	plt.show()

def getAreaUnderCurve(logfile):
	theFile = open(logfile, 'r')
	FILE = theFile.readlines()
	theFile.close()

	x = []
	for line in FILE:
			x.append(int(line.split('~')[1].strip()))
	y = np.array(x)
	area = trapz(y, dx=5)

	f = open("area.log", 'a')
	f.write(area)
	f.close()

getMemoryUsed(parameterforhistorical, pathforhistorical)
getSegmentCount(parameterforcoordinator, pathforcoordinator)
plotMemoryUsed()
plotSegmentCount()
getAverageLatency(parameterforbroker, pathforbroker)
getAreaUnderCurve("historicalmetrics.log")





