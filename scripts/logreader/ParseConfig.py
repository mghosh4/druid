import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.historicalmetric = ""
		self.coordinatormetric = ""
		self.brokermetric = ""
		self.logpath = ""
		self.numhistoricalnodes = ""
		self.numbrokernodes = ""
		self.numcoordinatornodes = ""
		self.resultfolder = ""


		if configFilePath is not  None:
			self.parseConfigFile(configFilePath)

	def parseConfigFile(self, configFilePath):
		with open(configFilePath) as f:
			for line in f:
				if line[0] == "#":
					continue
				
				if "," in line:
					words = line.split("=")
                                        key = words[0]
                                        value = words[1].replace("\n", "")
					values = value.split(",")
					if key == "HISTORICAL_METRIC":
                                                self.historicalmetric = values
					elif key == "COORDINATOR_METRIC":
                                                self.coordinatormetric = values
                                        elif key == "BROKER_METRIC":
                                                self.brokermetric = values
                                        elif key == "LOG_PATH":
                                                self.logpath = values
                                        elif key == "NUM_HISTORICAL_NODES":
                                                self.numhistoricalnodes = values
                                        elif key == "NUM_BROKER_NODES":
                                                self.numbrokernodes = values
                                        elif key == "NUM_COORDINATOR_NODES":
                                                self.numcoordinatornodes = values
                                        elif key == "RESULT_FOLDER":
                                                self.resultfolder = values


				else:
					words = line.split("=")
					key = words[0]
					value = words[1].replace("\n", "")
				
					if key == "HISTORICAL_METRIC":
						self.historicalmetric = value
					elif key == "COORDINATOR_METRIC":
						self.coordinatormetric = value
					elif key == "BROKER_METRIC":
						self.brokermetric = value
					elif key == "LOG_PATH":
						self.logpath = value
					elif key == "NUM_HISTORICAL_NODES":
						self.numhistoricalnodes = value
					elif key == "NUM_BROKER_NODES":
						self.numbrokernodes = value
					elif key == "NUM_COORDINATOR_NODES":
						self.numcoordinatornodes = value
					elif key == "RESULT_FOLDER":
						self.resultfolder = value

	def getHistoricalMetric(self):
		return self.historicalmetric

	def getCoordinatorMetric(self):
		return self.coordinatormetric

	def getBrokerMetric(self):
		return self.brokermetric

	def getLogPath(self):
		return self.logpath

	def getNumHistoricalNodes(self):
		return self.numhistoricalnodes

	def getNumBrokerNodes(self):
		return self.numbrokernodes

	def getNumCoordinatorNodes(self):
		return self.numcoordinatornodes

	def getResultFolder(self):
		return self.resultfolder
