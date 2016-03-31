from collections import Counter

class Query(object):
	def __init__(self, index, time):
		self.index = index
        self.startTime = time
		self.interval = "0000-00-00/p1d"
	#def show(self):
	#	print "Query %d: %s" % (self.index, ', '.join(x.info() for x in self.segmentList.iterkeys()))

	def getID(self):
		return self.index

	def getInterval(self):
		return self.interval

	def setInterval(self, newinterval):
		self.interval = newinterval