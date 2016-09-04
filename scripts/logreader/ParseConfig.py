import os
class ParseConfig:
	def __init__(self, configFilePath):
		self.parameterforbroker = ""
		self.parameterforhistorical = ""
		self.pathforhistorical = ""
		self.pathforbroker = ""
		self.timerangeforhistorical = ""
		self.timerangeforbroker = ""


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
				
				if key == "parameterforbroker":
					self.parameterforbroker = value
				elif key == "parameterforhistorical":
					self.parameterforhistorical = value
				elif key == "pathforhistorical":
					self.pathforhistorical = value
				elif key == "pathforbroker":
					self.pathforbroker = value
				elif key == "timerangeforhistorical":
					self.timerangeforhistorical = value
				elif key == "timerangeforbroker":
					self.timerangeforbroker = value

	def getParameterForHistorical(self):
		return self.parameterforhistorical

	def getParameterForBroker(self):
		return self.parameterforbroker

	def getPathForHistorical(self):
		return self.pathforhistorical

	def getPathForBroker(self):
		return self.pathforbroker

	def getTimeRangeForHistorical(self):
		return self.timerangeforhistorical

	def getTimeRangeForBroker(self):
		return self.timerangeforbroker
