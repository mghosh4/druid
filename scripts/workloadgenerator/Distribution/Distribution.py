import numpy
class Uniform(object):

	def generateDistribution(self, minSample, maxSample, numSamples):
		# start and end is inclusive
		return numpy.random.random_integers(minSample, maxSample, numSamples)

class Zipfian(object):

	def generateDistribution(self, minSample, maxSample, numSamples):
		shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
		zipfsample = self.randZipf(maxSample - minSample + 1, shape, numSamples)
		#print "Zipf List"
		#Utils.printlist(zipfsample)

		return [ x + minSample for x in zipfsample ]

	# Used code from stackoverflow link http://stackoverflow.com/questions/31027739/python-custom-zipf-number-generator-performing-poorly
	def randZipf(self, n, alpha, numSamples): 
	    	# Calculate Zeta values from 1 to n: 
	    	tmp = numpy.power( numpy.arange(1, n+1), -alpha )
	    	zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
	    	# Store the translation map: 
	    	distMap = [x / zeta[-1] for x in zeta]
	    	# Generate an array of uniform 0-1 pseudo-random values: 
	    	u = numpy.random.random(numSamples)
	    	# bisect them with distMap
	    	v = numpy.searchsorted(distMap, u)
	    	samples = [t-1 for t in v]
	    	return samples

class Latest(Zipfian):

	def generateDistribution(self, minSample, maxSample, numSamples):
		latestsample = super(Latest, self).generateDistribution(minSample, maxSample, numSamples)
		return [maxSample - x + minSample for x in latestsample]


class ScrambledZipfian(Zipfian):

	def generateDistribution(self, minSample, maxSample, numSamples):
		scrambledzipfiansample = super(ScrambledZipfian, self).generateDistribution(minSample, maxSample, numSamples)
		itemcount = maxSample - minSample + 1
		return [minSample + x % itemcount for x in scrambledzipfiansample]