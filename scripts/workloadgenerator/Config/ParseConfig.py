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
		self.brokernodeurl = ""
		self.brokerendpoint = ""
		self.minqueryperiod = 0
		self.maxqueryperiod = 0
		self.numqueries = 0
		self.earliestyear = 0
		self.earliestmonth = 0
		self.earliestday = 0
		self.earliesthour = 0
		self.earliestminute = 0
		self.earliestsecond = 0
		self.opspersecond = 0
		self.queryruntime = 0

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
				elif key == "brokernodeurl":
					self.brokernodeurl = value
				elif key == "brokerendpoint":
					self.brokerendpoint = value
				elif key == "minqueryperiod":
					self.minqueryperiod = int(value)
				elif key == "maxqueryperiod":
					self.maxqueryperiod = int(value)
				elif key == "numqueries":
					self.numqueries = int(value)
				elif key == "earliestyear":
					self.earliestyear = int(value)
				elif key == "earliestmonth":
					self.earliestmonth = int(value)
				elif key == "earliestday":
					self.earliestday = int(value)
				elif key == "earliesthour":
					self.earliesthour = int(value)
				elif key == "earliestminute":
					self.earliestminute = int(value)
				elif key == "earliestsecond":
					self.earliestsecond = int(value)
				elif key == "opspersecond":
					self.opspersecond = int(value)
				elif key == "queryruntime":
					self.queryruntime = float(value)

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

	def getBrokerNodeUrl(self):
		return self.brokernodeurl

	def getBrokerEndpoint(self):
		return self.brokerendpoint

	def getMinQueryPeriod(self):
		return self.minqueryperiod

	def getMaxQueryPeriod(self):
		return self.maxqueryperiod

	def getNumQueries(self):
		return self.numqueries

	def getEarliestYear(self):
		return self.earliestyear

	def getEarliestMonth(self):
		return self.earliestmonth

	def getEarliestDay(self):
		return self.earliestday

	def getEarliestHour(self):
		return self.earliesthour

	def getEarliestMinute(self):
		return self.earliestminute

	def getEarliestSecond(self):
		return self.earliestsecond

	def getOpsPerSecond(self):
		return self.opspersecond

	def getQueryRuntime(self):
		return self.queryruntime

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
		print "Broker Node Url : " + self.getBrokerNodeUrl()
		print "Broker Endpoint : " + self.getBrokerEndpoint()
		print "Min Query Period : " + self.getMinQueryPeriod()
		print "Max Query Period : " + self.getMaxQueryPeriod()
		print "Num Queries : " + self.getNumQueries()
		print "Earliest Year : " + self.getEarliestYear()
		print "Earliest Month : " + self.getEarliestMonth()
		print "Earliest Day : " + self.getEarliestDay()
		print "Earliest Hour : " + self.getEarliestHour()
		print "Earliest Minute : " + self.getEarliestMinute()
		print "Earliest Second : " + self.getEarliestSecond()
		print "Operations per second : " + self.getOpsPerSecond()
		print "Query Runtime : " + self.getQueryType()