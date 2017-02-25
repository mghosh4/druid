import os, sys
import numpy
import threading
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

def parseLogAndWriteMetrics(nodetype, metrics, numnodes, logpath, resultfolder, resultmetrics):
    nodemetric = DruidNodeLogReader(nodetype, metrics, numnodes, logpath, resultfolder)
    nodemetric.writeMetrics()
    resultmetrics[nodetype] = nodemetric

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

historicalmetrics = config.getHistoricalMetric()
brokermetrics = config.getBrokerMetric()
logpath = config.getLogPath()
num_h_nodes = config.getNumHistoricalNodes()
num_b_nodes = config.getNumBrokerNodes()
resultfolder = config.getResultFolder()
if not os.path.exists(resultfolder):
    os.makedirs(resultfolder)

### Parse Logs ###
resultmetrics = dict()
broker = threading.Thread(target=parseLogAndWriteMetrics, args=("broker", brokermetrics, num_b_nodes, logpath, resultfolder, resultmetrics))
broker.daemon = True
broker.start()
historical = threading.Thread(target=parseLogAndWriteMetrics, args=("historical", historicalmetrics, num_h_nodes, logpath, resultfolder, resultmetrics))
historical.daemon = True
historical.start()

broker.join()
historical.join()

brokermetric = resultmetrics["broker"]
historicalmetric = resultmetrics["historical"]

### Historical Metrics ###
stats = [sum, numpy.mean, max, min]
headerStr = "Time\tTotal\tMean\tMax\tMin\n"
aggValues = dict()
for metric in historicalmetrics:
    aggValues[metric] = historicalmetric.getAggregateStats(metric, stats)
    
    if "/" in metric:
        newmetrics = metric.replace("/", "-")
    else:
        newmetrics = metric

    filename = resultfolder + "/historical" + "-" + newmetrics + ".log"
    Utils.writeTimeSeriesMetricStats(filename, aggValues[metric], stats, headerStr)

stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95, Utils.percentile99]
headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\n"
for metric in historicalmetrics:
    overallStats = aggValues[metric][sum]
    if "/" in metric:
        newmetrics = metric.replace("/", "-")
    else:
        newmetrics = metric

    filename = resultfolder + "/historical" + "-" + newmetrics + ".cdf"
    Utils.writeCDF(filename, overallStats.values())

    statfile = resultfolder + "/historical" + "-" + newmetrics + ".log"
    metricstats = dict()
    for stat in stats:
        metricstats[stat] = stat(overallStats.values())

    Utils.writeOverallMetricStats(statfile, metricstats, stats, headerStr)

### Broker Metrics ###

##Query Runtime
stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95, Utils.percentile99]
headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\n"
overallStats = brokermetric.getOverallStats("query/time", stats)
newmetrics = "query/time".replace("/", "-")
filename = resultfolder + "/broker" + "-" + newmetrics + ".log"
Utils.writeOverallMetricStats(filename, overallStats, stats, headerStr)

for metric in ["query/time"]:
    overallStats = brokermetric.getOverallMetric(metric)
    if "/" in metric:
        newmetrics = metric.replace("/", "-")
    else:
        newmetrics = metric

    filename = resultfolder + "/broker" + "-" + newmetrics + ".cdf"
    Utils.writeCDF(filename, overallStats)

