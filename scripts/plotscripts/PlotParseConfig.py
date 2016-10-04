import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.historicalmetrics = ""
		self.brokermetrics = ""
		self.coordinatormetrics = ""
		self.throughputgraph = ""
		self.pathtoresults = ""
		self.numberofhistorical = ""
		self.numberofbroker = ""
		self.numberofcoordinator = ""
		self.distributions = ""
		self.druidversion = ""


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
				if("," in value):
					values = value.split(",")
				else:
					values = []
					if( value != ""):
						values.append(value)				
				if key == "historicalmetrics":
					self.historicalmetrics = values
				elif key == "brokermetrics":
					self.brokermetrics = values
				elif key == "coordinatormetrics":
					self.coordinatormetrics = values
				elif key == "throughputgraph":
					self.throughputgraph = values
				elif key == "numberofhistorical":
					self.numberofbroker = values
				elif key == "numberofcoordinator":
					self.numberofcoordinator = values
				elif key == "numberofbroker":
					self.numberofbroker = values
				elif key == "distributions":
					self.distributions = values
				elif key == "druidversion":
					self.druidversion = values
				elif key == "pathtoresults":
					self.pathtoresults = values

	def getHistoricalMetrics(self):
		return self.historicalmetrics

	def getBrokerMetrics(self):
		return self.brokermetrics

	def getCoordinatorMetrics(self):
		return self.coordinatormetrics

	def getThroughputGraph(self):
		return self.throughputgraph

	def getNumberOfHistorical(self):
		return self.numberofhistorical

	def getNumberOfBroker(self):
		return self.numberofbroker

	def getNumberOfCoordinator(self):
		return self.numberofcoordinator

	def getDistributions(self):
		return self.distributions

	def getDruidVersion(self):
		return self.druidversion

	def getPathToResults(self):
		return self.pathtoresults
