from datetime import datetime

class Query(object):
	def __init__(self, index, time):
		self.index = index
		self.startTime = time
		self.interval = ""
		self.tx = datetime.now()
		self.rx = datetime.now()

	def getID(self):
		return self.index

	def getInterval(self):
		return self.interval

	def setInterval(self, newinterval):
		self.interval = newinterval

	def setTxTime(self, time):
		self.tx = time

	def setRxTime(self, time):
		self.rx = time

	def getExecutionTime(self):
		return (self.rx-self.tx).total_seconds()*1000
		
