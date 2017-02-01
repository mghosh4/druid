import os, sys
import numpy

from ParseConfig import ParseConfig
from DruidNodeLogReader import DruidNodeLogReader
from Utils import Utils

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

### Historical Metrics ###
stats = [sum, numpy.mean, max, min]
headerStr = "Time\tTotal\tMean\tMax\tMin\n"

for metric in ["segment/count", "sys/mem/used"]:
	aggValues = historicalmetrics.getAggregateStats(metric, stats)
	
	if "/" in metric:
		newmetrics = metric.replace("/", "-")
	else:
		newmetrics = metric
		
	filename = resultfolder + "/historical" + "-" + newmetrics + ".log"
	Utils.writeTimeSeriesMetricStats(filename, aggValues, stats, headerStr)

for metric in ["query/time", "query/wait/time"]:
	overallStats = historicalmetrics.getOverallMetric(metric)
	if "/" in metric:
		newmetrics = metric.replace("/", "-")
	else:
		newmetrics = metric

	filename = resultfolder + "/historical" + "-" + newmetrics + ".cdf"
	Utils.writeCDF(filename, overallStats)

### Broker Metrics ###
stats = [numpy.mean, numpy.median, Utils.percentile99, Utils.percentile90, Utils.percentile75]
headerStr = "\tMean\tMedian\t99th Percentile\t95th Percentile\t75th Percentile\n"

for metric in ["query/time"]:
	overallStats = brokermetrics.getOverallStats(metric, stats)
	if "/" in metric:
		newmetrics = metric.replace("/", "-")
	else:
		newmetrics = metric
		
	filename = resultfolder + "/broker" + "-" + newmetrics + ".log"
	Utils.writeOverallMetricStats(filename, overallStats, stats, headerStr)

for metric in ["query/time"]:
	overallStats = brokermetrics.getOverallMetric(metric)
	if "/" in metric:
		newmetrics = metric.replace("/", "-")
	else:
		newmetrics = metric

	filename = resultfolder + "/broker" + "-" + newmetrics + ".cdf"
	Utils.writeCDF(filename, overallStats)

### Coordinator Metrics ###
