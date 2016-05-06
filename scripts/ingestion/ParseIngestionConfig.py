import os
class ParseIngestionConfig:
	def __init__(self, configFilePath):
		self.kafkapath=""
		self.datatype=""
		self.datafilepath=""
		self.kafkatopic=""
		self.delimiter=""
		self.kafkahost=""

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
				
				if key == "kafkapath":
					self.kafkapath = value
				elif key == "datatype":
					self.datatype = value
				elif key == "datafilepath":
					self.datafilepath = value
				elif key == "kafkatopic":
					self.kafkatopic = value
				elif key == "delimiter":
					self.delimiter = value				
				elif key == "kafkahost":
					self.kafkahost = value

	def getKafkaPath(self):
		return self.kafkapath

	def getDataType(self):
		return self.datatype

	def getDataFilePath(self):
		return self.datafilepath

	def getKafkaTopic(self):
		return self.kafkatopic

	def getDelimiter(self):
		return self.delimiter

	def getKafkaHost(self):
		return self.kafkahost



	def printConfig(self):
		print "Config details"
		print "Kafka Path : " + self.getKafkaPath()
		print "Data Type : " + self.getDataType()
		print "Data File Path : " + self.getDataFilePath()
		print "Kafka Topic : " + self.getKafkaTopic()
		print "Delimiter : " + self.getDelimiter()
		print "Kafka Host : " + self.getKafkaHost()
