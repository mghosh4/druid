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
    plt.clf()
    plt.hist(data, bins=numpy.arange(min(data), max(data)+1))
    #p, x = numpy.histogram(data) # bin it into 10 bins
    #x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
    #f = UnivariateSpline(x, p, data=numBins)
    #plt.plot(x, p)
    plt.title('Druid distribution')
    # plt.ylabel('Total segment access')
    # plt.xlabel('Time')
    # plt.ylim(0, float(1.25*max(y)))
    # plt.grid(True) 
    plt.savefig(filename)

class Druid(Zipfian):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        # use 9:1 split of old segment vs new segment range
        numLatestSegmentSamples = numSamples/10
        numOldSegmentSamples = numSamples - numLatestSegmentSamples 
        oldsamples = super(Druid, self).generateDistribution(minSample, (maxSample-(timedelta(minutes=1).total_seconds())), numOldSegmentSamples, popularityList, logger)

        latestsamples = super(Druid, self).generateDistribution(minSample, maxSample, numLatestSegmentSamples, popularityList, logger)
         
        allsamples = [(maxSample-(timedelta(minutes=1).total_seconds())) - x + minSample for x in oldsamples] + [maxSample - x + minSample for x in latestsamples]
        
        #self.plotDistribution(allsamples, 'druid_distribution.png', 'Druid-Distribution')
        
        # convert zipfian to druid distribution
        #numBins = 10 
        #binSize = len(allsamples)/numBins
        #chunk1 = datalatest[0:binSize+1]
        #chunk1.reverse()
        #chunk2 = datalatest[binSize:-1]
        #datadruid = chunk2 + chunk1
        #print datalatest[0:50]
        #print datalatest[-50:-1]
        #print datadruid[0:50]
        #print datadruid[-50:-1]
        #abd

        #self.plotDistribution(datadruid, 'druid_distribution.png', 'Druid-Distribution')

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

class Beta(object):
    
    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        alpha = 4
        beta = 50
        return [minSample + round(x * (maxSample - minSample)) for x in numpy.random.beta(alpha, beta, numSamples)]

class LogNormal(object):
    
    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        mean = 5
        var = 1
        distrib = numpy.random.lognormal(mean, var, numSamples)
        mindist = min(distrib)
        maxdist = max(distrib)
        dstrange = maxdist - mindist
        smplrange = maxSample - minSample
        return [minSample + round((x - mindist) * smplrange / dstrange) for x in distrib]
        

class Inverse(LogNormal):
    count = 0
    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        betasample = super(Inverse, self).generateDistribution(minSample, maxSample, numSamples, popularityList, logger)
        #print betasample
        #betadistribfilename = 'beta_distribution' + str(self.count) + '.png'
        #plotDistribution(betasample, betadistribfilename)
        #self.count = self.count + 1
        #print maxSample, minSample
        invbetasample = [maxSample - x + minSample for x in betasample]
        #print invbetasample
        #invbetadistribfilename = 'inv_beta_distribution' + str(self.count) + '.png'
        #plotDistribution(invbetasample, invbetadistribfilename)
        #self.count = self.count + 1
        return invbetasample
