import os
class ParseConfig:
    def __init__(self, configFilePath):
        self.datasource = ""
        self.granularity = ""
        self.aggregations = ""
        self.dimension = ""
        self.metric = ""
        self.threshold = ""
        self.filter = ""
        self.distribution = ""
        self.postaggregations = ""
        self.accessdistribution = ""
        self.perioddistribution = ""
        self.querytype = ""
        self.queryratio = ""
        self.brokernodeurl = ""
        self.brokerendpoint = ""
        self.minops = 0
        self.maxops = 0
        self.logfolder = ""
        self.runtime = 0
        self.segmentpopularityinterval = 0
        self.isbatch = 0
        self.filename = ""

        if configFilePath is not  None:
            self.parseConfigFile(configFilePath)

    def parseConfigFile(self, configFilePath):
        with open(configFilePath) as f:
            for line in f:
                if line[0] == "#":
                    continue
                
                words = line.split("=")
                key = words[0]
                value = words[1].replace("\n", "")
                
                if key == "datasource":
                    self.datasource = value
                elif key == "granularity":
                    self.granularity = value
                elif key == "aggregations":
                    self.aggregations = value
                elif key == "dimension":
                    self.dimension = value
                elif key == "metric":
                    self.metric = value
                elif key == "threshold":
                    self.threshold = value
                elif key == "filter":
                    self.filter = value
                elif key == "distribution":
                    self.distribution = value
                elif key == "postaggregations":
                    self.postaggregations = value
                elif key == "accessdistribution":
                    self.accessdistribution = value
                elif key == "perioddistribution":
                    self.perioddistribution = value
                elif key == "querytype":
                    self.querytype = value
                elif key == "queryratio":
                    self.queryratio = value
                elif key == "brokernodeurl":
                    self.brokernodeurl = value
                elif key == "brokerendpoint":
                    self.brokerendpoint = value
                elif key == "minops":
                    self.minops = int(value)
                elif key == "maxops":
                    self.maxops = int(value)
                elif key == "logfolder":
                    self.logfolder = value
                elif key == "runtime":
                    self.runtime = int(value)
                elif key == "segmentpopularityinterval":
                    self.segmentpopularityinterval = int(value)
                elif key == "isbatch":
                    self.isbatch = int(value)
                elif key == "filename":
                    self.filename = value

    def getDataSource(self):
        return self.datasource

    def getGranularity(self):
        return self.granularity

    def getAggregations(self):
        return self.aggregations

    def getDimension(self):
        return self.dimension

    def getMetric(self):
        return self.metric

    def getThreshold(self):
        return self.threshold

    def getFilter(self):
        return self.filter

    def getDistribution(self):
        return self.distribution

    def getPostAggregations(self):
        return self.postaggregations

    def getAccessDistribution(self):
        return self.accessdistribution

    def getPeriodDistribution(self):
        return self.perioddistribution

    def getQueryType(self):
        return self.querytype

    def getQueryRatio(self):
        return self.queryratio

    def getBrokerNodeUrl(self):
        return self.brokernodeurl

    def getBrokerEndpoint(self):
        return self.brokerendpoint

    def getMinOpsPerSecond(self):
        return self.minops

    def getMaxOpsPerSecond(self):
        return self.maxops

    def getLogFolder(self):
        return self.logfolder

    def getRunTime(self):
        return self.runtime

    def getSegmentPopularityInterval(self):
        return self.segmentpopularityinterval

    def getBatchExperiment(self):
        return self.isbatch > 0

    def getFileName(self):
        return self.filename

    def printConfig(self):
        print "Config details"
        print "Data Source : " + self.getDataSource()
        print "Granularity : " + self.getGranularity()
        print "Aggregations : " + self.getAggregations()
        print "Dimension : " + self.getDimension()
        print "Metric : " + self.getMetric()
        print "Threshold : " + self.getThreshold()
        print "Filter : " + self.getFilter()
        print "Distribution : " + self.getDistribution()
        print "Post Aggregations : " + self.getPostAggregations()
        print "Access Distribution : " + self.getAccessDistribution()
        print "Period Distribution : " + self.getPeriodDistribution()
        print "Query Type : " + self.getQueryType()
        print "Query Ratio : " + self.getQueryRatio()
        print "Broker Node Url : " + self.getBrokerNodeUrl()
        print "Broker Endpoint : " + self.getBrokerEndpoint()
        print "Min Operations per second : " + self.getMinOpsPerSecond()
        print "Max Operations per second : " + self.getMaxOpsPerSecond()
        print "Log Folder : " + self.getLogFolder()
        print "Runtime : " + self.getRunTime()
        print "Is Batch Experiment : " + self.getBatchExperiment()
        print "File Name : " + self.getFileName()
