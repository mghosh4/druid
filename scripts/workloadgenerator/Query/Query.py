class Query(object):
	def __init__(self, index, time):
		self.index = index
		self.startTime = time
		self.interval = ""

	def getID(self):
		return self.index

	def getInterval(self):
		return self.interval

	def setInterval(self, newinterval):
		self.interval = newinterval
		
