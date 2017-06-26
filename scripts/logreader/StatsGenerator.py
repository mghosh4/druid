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
    if len(metrics) > 0:
        nodemetric = DruidNodeLogReader(nodetype, metrics, numnodes, logpath, resultfolder)
        nodemetric.writeMetrics()
        resultmetrics[nodetype] = nodemetric

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

historicalmetrics = config.getHistoricalMetric()
brokermetrics = config.getBrokerMetric()
coordinatormetrics = config.getCoordinatorMetric()
logpath = config.getLogPath()
num_h_nodes = config.getNumHistoricalNodes()
num_b_nodes = config.getNumBrokerNodes()
num_c_nodes = config.getNumCoordinatorNodes()
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

coordinator = threading.Thread(target=parseLogAndWriteMetrics, args=("coordinator", coordinatormetrics, num_c_nodes, logpath, resultfolder, resultmetrics))
coordinator.daemon = True
coordinator.start()

broker.join()
historical.join()
coordinator.join()

## Historical Metrics ###
if "historical" in resultmetrics:
    stats = [sum, numpy.mean, max, min]
    headerStr = "Time\tTotal\tMean\tMax\tMin\n"
    aggValues = dict()
    for metric in historicalmetrics:
        aggValues[metric] = resultmetrics["historical"].getAggregateStats(metric, stats)
       
        if "/" in metric:
            newmetrics = metric.replace("/", "-")
        else:
            newmetrics = metric
    
        filename = resultfolder + "/historical" + "-" + newmetrics + ".log"
        Utils.writeTimeSeriesMetricStats(filename, aggValues[metric], stats, headerStr)
    
    stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95, Utils.percentile99, numpy.mean]
    headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\n"
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

# write the HN node to hostname map to hn-to-hostname-map.log
mapfile = resultfolder + "/historical-to-hostname-map.log"
mFile = open(mapfile, 'w')
for i in range(0,num_h_nodes):
    filename = logpath + "/historical-"+str(i)+".log"
    for line in open(filename):
        if "host=" in line:
            mapOutput = "HN-"+str(i)+"\t"+line.split("'")[-2]+"\n"
            mFile.write(mapOutput)
            break
mFile.close()

## Broker Metrics ###
if "broker" in resultmetrics:
    #Query Runtime
    stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95, Utils.percentile99, numpy.mean]
    headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\n"
    overallStats = resultmetrics["broker"].getOverallStats("query/time", stats)
    newmetrics = "query/time".replace("/", "-")
    filename = resultfolder + "/broker" + "-" + newmetrics + ".log"
    Utils.writeOverallMetricStats(filename, overallStats, stats, headerStr)
    
    for metric in ["query/time"]:
        overallStats = resultmetrics["broker"].getOverallMetric(metric)
        if "/" in metric:
            newmetrics = metric.replace("/", "-")
        else:
            newmetrics = metric
    
        filename = resultfolder + "/broker" + "-" + newmetrics + ".cdf"
        Utils.writeCDF(filename, overallStats)

## Coordinator Metrics
if "coordinator" in resultmetrics:
    stats = [sum, numpy.mean, max, min]
    headerStr = "Time\tTotal\tMean\tMax\tMin\n"
    aggValues = dict()
    for metric in coordinatormetrics:
        aggValues[metric] = resultmetrics["coordinator"].getAggregateStats(metric, stats)
       
        if "/" in metric:
            newmetrics = metric.replace("/", "-")
        else:
            newmetrics = metric
    
        filename = resultfolder + "/coordinator" + "-" + newmetrics + ".log"
        Utils.writeTimeSeriesMetricStats(filename, aggValues[metric], stats, headerStr)
    
    stats = [numpy.median, Utils.percentile75, Utils.percentile90, Utils.percentile95, Utils.percentile99, numpy.mean]
    headerStr = "Median\t75th Percentile\t90th Percentile\t95th Percentile\t99th Percentile\tMean\n"
    for metric in coordinatormetrics:
        overallStats = aggValues[metric][sum]
        if "/" in metric:
            newmetrics = metric.replace("/", "-")
        else:
            newmetrics = metric
    
        filename = resultfolder + "/coordinator" + "-" + newmetrics + ".cdf"
        Utils.writeCDF(filename, overallStats.values())
    
        statfile = resultfolder + "/coordinator" + "-" + newmetrics + ".log"
        metricstats = dict()
        for stat in stats:
            metricstats[stat] = stat(overallStats.values())
    
        Utils.writeOverallMetricStats(statfile, metricstats, stats, headerStr)
