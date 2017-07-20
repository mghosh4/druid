import numpy
import matplotlib.pyplot as plt
from scipy.interpolate import UnivariateSpline
from datetime import datetime, timedelta

class Uniform(object):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList):
        # start and end is inclusive
        return numpy.random.random_integers(minSample, maxSample, numSamples)

class Zipfian(object):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
        zipfsample = self.randZipf(maxSample - minSample + 1, shape, numSamples, logger)
        #print "Zipf List"
        #Utils.printlist(zipfsample)
	#logger.info("Distribution Samples "+str(zipfsample))

        return [ x + minSample for x in zipfsample ]

    # Used code from stackoverflow link http://stackoverflow.com/questions/31027739/python-custom-zipf-number-generator-performing-poorly
    def randZipf(self, n, alpha, numSamples, logger):
        #logger.info("Dist params "+str(n)+", "+str(alpha))
	# Calculate Zeta values from 1 to n:
        tmp = numpy.power( numpy.arange(1, n+1), -alpha )
	#logger.info("tmp "+str(tmp))
        zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
	#logger.info("zeta "+str(zeta))
        # Store the translation map:
        distMap = [x / zeta[-1] for x in zeta]
	#logger.info("distMap "+str(distMap))
        # Generate an array of uniform 0-1 pseudo-random values:
        u = numpy.random.random(numSamples)
        # bisect them with distMap
        v = numpy.searchsorted(distMap, u)
	#logger.info("U, V "+str(u)+" $ "+str(v))
        samples = [t-1 for t in v]
        return samples

def plotDistribution(data, filename):

    numBins = len(data)/10
    p, x = numpy.histogram(data, bins=len(data)/10) # bin it into 10 bins
    x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
    f = UnivariateSpline(x, p, data=numBins)
    plt.plot(x, f(x))
    plt.title('Druid distribution')
    # plt.ylabel('Total segment access')
    # plt.xlabel('Time')
    # plt.ylim(0, float(1.25*max(y)))
    # plt.grid(True) 
    plt.savefig(filename)

class Druid(Zipfian):

    def plotDistribution(self, data, figfilename, figtitle):

        #numBins = len(data)/10
        #p, x = numpy.histogram(data, bins=len(data)/10) # bin it into 10 bins
        #x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
        #f = UnivariateSpline(x, p, s=numBins)
        #plt.plot(x, f(x))
        plt.plot(list(range(0,len(data))), data, 'b,')
        plt.title(figtitle)
        # plt.ylabel('Total segment access')
        # plt.xlabel('Time')
        # plt.ylim(0, float(1.25*max(y)))
        # plt.grid(True)
        plt.savefig(figfilename)

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        # use 9:1 split of old segment vs new segment range
        numLatestSegmentSamples = numSamples/10
        numOldSegmentSamples = numSamples - numLatestSegmentSamples
        oldsamples = super(Druid, self).generateDistribution(minSample, (maxSample-(timedelta(minutes=1).total_seconds())), numOldSegmentSamples, popularityList, logger)
        latestsamples = super(Druid, self).generateDistribution(minSample, maxSample, numLatestSegmentSamples, popularityList, logger)
        allsamples = [(maxSample-(timedelta(minutes=1).total_seconds())) - x + minSample for x in oldsamples] + [maxSample - x + minSample for x in latestsamples]

        #self.plotDistribution(allsamples, 'druid_distribution.png', 'Druid-Distribution')

        return allsamples

class DynamicZipfian(object):

    def generateDistribution(self, minSample, maxSample, numSamples, indexList):
        shape = 1.2   # the distribution shape parameter, also known as `a` or `alpha`
        #update the index list first
        history = list()
        #print "minSample %s" % minSample
        #print "maxSample %s" % int(maxSample)
        for i in range(minSample, int(maxSample)+1):
            history.append(i)
        #print "history:"
        print str(history)
        zipfGenerator = Zipfian()
        delta=int(maxSample-minSample+1)
        idxLength = len(indexList)
        if delta>idxLength:
            #print "delta %s" % delta
            #print "idxLength %s" % idxLength
            for i in range(0, delta-idxLength):
                currIdx=i+len(indexList)
                samplelist = list()
                samplelist = self.randZipf(len(indexList), shape, 1)
                zipfSample = samplelist[0]
                indexList.insert(zipfSample, currIdx)
                      
        ranksample = self.randZipf(len(indexList), shape, numSamples)
        #print "length of indexList: %s"% len(indexList)
        print str(ranksample)
        #indexsample = [ indexList[x] for x in ranksample ]
        #print "Zipf List"
        #Utils.printlist(zipfsample)append

        return [ history[y] + minSample for y in ranksample ]

    def randZipf(self, n, alpha, numSamples):
        tmp = numpy.power( numpy.arange(1, n+1), -alpha )
        zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
        distMap = [x / zeta[-1] for x in zeta]
        u = numpy.random.random(numSamples)
        v = numpy.searchsorted(distMap, u)
        samples = [t-1 for t in v]
        return samples


class Latest(Zipfian):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        latestsample = super(Latest, self).generateDistribution(minSample, maxSample, numSamples, popularityList, logger)
        return [maxSample - x + minSample for x in latestsample]


class ScrambledZipfian(Zipfian):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        scrambledzipfiansample = super(ScrambledZipfian, self).generateDistribution(minSample, maxSample, numSamples, popularityList, logger)
        itemcount = maxSample - minSample + 1
        return [minSample + x % itemcount for x in scrambledzipfiansample]
