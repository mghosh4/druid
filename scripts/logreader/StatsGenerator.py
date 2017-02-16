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
coordinatormetric = DruidNodeLogReader("coordinator", coordinatormetrics, num_c_nodes, logpath, resultfolder)
brokermetric = DruidNodeLogReader("broker", brokermetrics, num_b_nodes, logpath, resultfolder)
historicalmetric = DruidNodeLogReader("historical", historicalmetrics, num_h_nodes, logpath, resultfolder)

### Print Metrics ###
coordinatormetric.writeMetrics()
brokermetric.writeMetrics()
historicalmetric.writeMetrics()

### Historical Metrics ###
stats = [sum, numpy.mean, max, min]
headerStr = "Time\tTotal\tMean\tMax\tMin\n"

for metric in historicalmetrics:
    aggValues = historicalmetric.getAggregateStats(metric, stats)
    
    if "/" in metric:
        newmetrics = metric.replace("/", "-")
    else:
        newmetrics = metric

    filename = resultfolder + "/historical" + "-" + newmetrics + ".log"
    Utils.writeTimeSeriesMetricStats(filename, aggValues, stats, headerStr)

### Broker Metrics ###

##Query Runtime
stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95]
headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\n"
overallStats = brokermetric.getOverallStats("query/time", stats)
newmetrics = "query/time".replace("/", "-")
filename = resultfolder + "/broker" + "-" + newmetrics + ".log"
Utils.writeOverallMetricStats(filename, overallStats, stats, headerStr)

##Query Throughput
#totalqueries = len(brokermetric.getOverallMetric("query/time"))
#throughputstats = dict()
#percentage = [0.5, 0.75, 0.9, 0.95]
#count = 0
#for stat in stats:
#    print(count, stat)
#    throughputstats[stat] = totalqueries * percentage[count] / overallStats[stat]
#    count = count + 1
#filename = resultfolder + "/query-throughput.log"
#Utils.writeOverallMetricStats(filename, throughputstats, stats, headerStr)

for metric in ["query/time"]:
	overallStats = brokermetric.getOverallMetric(metric)
	if "/" in metric:
		newmetrics = metric.replace("/", "-")
	else:
		newmetrics = metric

	filename = resultfolder + "/broker" + "-" + newmetrics + ".cdf"
	Utils.writeCDF(filename, overallStats)

### Coordinator Metrics ###
