import os, sys

from ParseConfig import ParseConfig
from DruidNodeLogReader import DruidNodeLogReader

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

### Parse Logs ###
coordinatormetrics = DruidNodeLogReader("coordinator", coordinatormetrics, num_c_nodes, logpath, resultfolder)
brokermetrics = DruidNodeLogReader("broker", brokermetrics, num_b_nodes, logpath, resultfolder)
historicalmetrics = DruidNodeLogReader("historical", historicalmetrics, num_h_nodes, logpath, resultfolder)

### Print Metrics ###
coordinatormetrics.writeMetrics()
brokermetrics.writeMetrics()
historicalmetrics.writeMetrics()

def getMemoryUsed(parameter,logfile):
	Writer(RunLogReader(parameter, logfile), resultfolder + "historicalmetrics.log")

def getSegmentCount(parameter, logfile):
	Writer(RunLogReader(parameter, logfile), resultfolder + "coordinatormetrics.log")

def getAverageLatency():
	dictionary = dict()
	theFile = open(resultfolder + "broker-0-query-time.log",'r')
    FILE = theFile.readlines()
    theFile.close() 

	totaltime = 0
	count = 0
	for line in FILE:
		totaltime += int(line.split("~")[1].strip())
		count += 1
	    f = open(resultfolder + "averagelatency.log", 'a')
        f.write(str(totaltime/count))
        f.close()

getAverageLatency()
