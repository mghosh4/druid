from Distribution import *
class DistributionFactory(object):

	@staticmethod
	def createSegmentDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return ScrambledZipfian()
		elif distribution == "dynamiczipf":
			return DynamicZipfian()
		elif distribution == "latest":
			return Latest()
		elif distribution == "druid":
			return Druid()
	
	@staticmethod
	def createSizeDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return Zipfian()