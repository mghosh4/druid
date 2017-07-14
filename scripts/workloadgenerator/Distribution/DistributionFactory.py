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
		elif distribution == "beta":
			return Beta()
		elif distribution == "inverse":
			return Inverse()
	
	@staticmethod
	def createSizeDistribution(distribution):
		if distribution == "uniform":
			return Uniform()
		elif distribution == "zipfian":
			return Zipfian()
