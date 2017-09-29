import numpy
import matplotlib.pyplot as plt
from scipy.interpolate import UnivariateSpline
from datetime import datetime, timedelta
from random import randint

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

    def genTwoSegmentPopularitySamples(self, minSample, maxSample, numSamples, popularityList, logger):
        minSegment = minSample
        maxSegment = maxSample+60
        range = maxSegment - minSegment
        # ingestAndRunWorkloadGen.sh has a 2min sleep. By the time this function gets called, 2+ segments should already be ingested
        popularSegmentSampleList = []
        risingSegmentSampleList = []
        fallingSegmentSampleList = []

        if range <= 60:
            #segment 0 is popular (100% samples)
            numPopularSegmentSamples = numSamples
            popularsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numPopularSegmentSamples, popularityList, logger)
            popularSegmentSampleList = [maxSegment - x + maxSegment-60 for x in popularsamples]
        elif range > 60 and range <= 120:
            # segment 0 is popular (90% samples), 1 is rising (10% samples)
            numRisingSegmentSamples = numSamples/10
            numPopularSegmentSamples = numSamples - numRisingSegmentSamples

            popularsamples = super(Druid, self).generateDistribution(maxSegment-2*60, maxSegment-60, numPopularSegmentSamples, popularityList, logger)
            popularSegmentSampleList = [maxSegment-60 - x + maxSegment-2*60 for x in popularsamples]

            risingsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numRisingSegmentSamples, popularityList, logger)
            risingSegmentSampleList = [maxSegment-60 - x + maxSegment for x in risingsamples]
        else:
            # if range == 180, segment 0 and 1 is popular (total 90% samples), 2 is rising (10% samples)
            # if range == 240, segment 0 is falling(10% samples), 1 and 2 is popular(80% samples), 3 is rising (10% samples)
            numFallingSegmentSamples = 0
            if range >= 240:
                numFallingSegmentSamples = numSamples/10
                fallingsamples = super(Druid, self).generateDistribution(maxSegment-4*60, maxSegment-3*60, numFallingSegmentSamples, popularityList, logger)
                fallingSegmentSampleList = [maxSegment-3*60 - x + maxSegment-4*60 for x in fallingsamples]

            numRisingSegmentSamples = numSamples/10
            numPopularSegmentSamples = numSamples - numRisingSegmentSamples - numFallingSegmentSamples

            popularsamples_0 = super(Druid, self).generateDistribution(maxSegment-3*60, maxSegment-2*60, numPopularSegmentSamples/2, popularityList, logger)
            popularSegmentSampleList_0 = [maxSegment-2*60 - x + maxSegment-3*60 for x in popularsamples_0]

            popularsamples_1 = super(Druid, self).generateDistribution(maxSegment-2*60, maxSegment-60, numPopularSegmentSamples/2, popularityList, logger)
            popularSegmentSampleList_1 = [maxSegment-60 - x + maxSegment-2*60 for x in popularsamples_1]
            popularSegmentSampleList = popularSegmentSampleList_0 + popularSegmentSampleList_1

            risingsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numRisingSegmentSamples, popularityList, logger)
            risingSegmentSampleList = [maxSegment - x + maxSegment-60 for x in risingsamples]

        #self.plotDistribution(allsamples, 'druid_distribution.png', 'Druid-Distribution')

        allSamples = fallingSegmentSampleList+popularSegmentSampleList+risingSegmentSampleList
        numpy.random.shuffle(allSamples)
        logger.info("Samples for minSample maxSample "+str(minSample)+" "+str(maxSample)+str(allSamples))
        return allSamples

    def genOneSegmentPopularitySamples(self, minSample, maxSample, numSamples, popularityList, logger):
        minSegment = minSample
        maxSegment = maxSample+60
        range = maxSegment - minSegment
        # ingestAndRunWorkloadGen.sh has a 2min sleep. By the time this function gets called, 2+ segments should already be ingested
        popularSegmentSampleList = []
        risingSegmentSampleList = []
        fallingSegmentSampleList = []

        if range <= 60:
            # segment 0 is popular (100% samples)
            numPopularSegmentSamples = numSamples
            popularsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numPopularSegmentSamples, popularityList, logger)
            popularSegmentSampleList = [maxSegment - x + maxSegment-60 for x in popularsamples]
        elif range > 60 and range <= 120:
            # segment 0 is popular (90% samples), 1 is rising (10% samples)
            numRisingSegmentSamples = numSamples/10
            numPopularSegmentSamples = numSamples - numRisingSegmentSamples
            popularsamples = super(Druid, self).generateDistribution(maxSegment-2*60, maxSegment-60, numPopularSegmentSamples, popularityList, logger)
            popularSegmentSampleList = [maxSegment-60 - x + maxSegment-2*60 for x in popularsamples]
            risingsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numRisingSegmentSamples, popularityList, logger)
            risingSegmentSampleList = [maxSegment-60 - x + maxSegment for x in risingsamples]
        else:
            # segment 0 is falling(10% samples), 1 is popular(80% samples), 2 is rising (10% samples)
            numFallingSegmentSamples = numSamples/10
            fallingsamples = super(Druid, self).generateDistribution(maxSegment-3*60, maxSegment-2*60, numFallingSegmentSamples, popularityList, logger)
            fallingSegmentSampleList = [maxSegment-2*60 - x + maxSegment-3*60 for x in fallingsamples]

            numRisingSegmentSamples = numSamples/10
            numPopularSegmentSamples = numSamples - numRisingSegmentSamples - numFallingSegmentSamples

            popularsamples = super(Druid, self).generateDistribution(maxSegment-2*60, maxSegment-60, numPopularSegmentSamples, popularityList, logger)
            popularSegmentSampleList = [maxSegment-60 - x + maxSegment-2*60 for x in popularsamples]

            risingsamples = super(Druid, self).generateDistribution(maxSegment-60, maxSegment, numRisingSegmentSamples, popularityList, logger)
            risingSegmentSampleList = [maxSegment - x + maxSegment-60 for x in risingsamples]

        #self.plotDistribution(allsamples, 'druid_distribution.png', 'Druid-Distribution')

        allSamples = fallingSegmentSampleList+popularSegmentSampleList+risingSegmentSampleList
        numpy.random.shuffle(allSamples)
        logger.info("Samples for minSample maxSample "+str(minSample)+" "+str(maxSample)+str(allSamples))
        return allSamples

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        numPopularSegments = 1 # code works only for values 1 and 2
        if numPopularSegments == 1:
            return self.genOneSegmentPopularitySamples(minSample, maxSample, numSamples, popularityList, logger)
        elif numPopularSegments == 2:
            return self.genTwoSegmentPopularitySamples(minSample, maxSample, numSamples, popularityList, logger)
        else:
            logger.info("Error: unsupported numPopularSegments "+str(numPopularSegments))

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





class Yahoo(object):

    def plotDistribution(self, data, figfilename, figtitle):

        plt.plot(list(range(0,len(data))), data, 'b')
        plt.title(figtitle)
        #print data
        plt.show()
        #plt.savefig(figfilename)

    def readDistFromFile(self, filename):
        accessDict = {}
        f = open(filename, 'rU')
        for line in f:
            lineArr = line.split(" ")
            ts_str = lineArr[0] + " " + lineArr[1]
            ts = datetime.strptime(ts_str[:19], '%Y-%m-%d %H:%M:%S')
            ts_long = (ts - datetime(2017,9,27)).total_seconds()
            access = int(lineArr[2])
            accessDict[ts_long] = access
        return accessDict

    def generateBinBasedDistribution(self, minSample, maxSample, numSamples, popularityList, binCount, accessDict, logger):
        binToProb = [] # bin id -> probablity of the bin
        binToSegment = {} # bin id -> segment
        binToAccess = {} # bin id -> probability

        # calculate per bin size
        binSize = len(accessDict) / binCount

        # add segments to each map
        currBinIdx = 0
        binSum = 0
        for key in sorted(accessDict):
            if currBinIdx not in binToAccess: # new bin that hasnt been created yet
                binToAccess[currBinIdx] = []
                binToSegment[currBinIdx] = []
                binSum = 0 # reset binSum
            binToSegment[currBinIdx].append(key)
            binToAccess[currBinIdx].append(accessDict[key])
            binSum += accessDict[key]

            if currBinIdx < binCount-1: # if not at the last bin
                if len(binToAccess[currBinIdx]) >= binSize: # move on to the next bin
                    binToProb.append(binSum)
                    currBinIdx += 1
        binToProb.append(binSum) # append the last binSum

        retPositions = []
        for i in xrange(numSamples):
            binPicked = self.pickByProbability(binToProb) # return the index of picked bin
            #print binPicked
            positionSegPicked = self.pickByProbability(binToAccess[binPicked])
            print positionSegPicked
            relativePosition = (positionSegPicked+1)*(maxSample-minSample)/(len(binToAccess[binPicked]))
            retPositions.append(relativePosition)
        #print "rest positions"
        #print retPositions
        return [minSample + x for x in retPositions]


    def pickByProbability(self, binToProb): # binary search to get the desired probability
        binSum = sum(binToProb)
        #print "binsum " + str(binSum)
        cumulative = numpy.cumsum(binToProb)
        seed = randint(1, binSum)
        #print "seed " + str(seed)
        #print cumulative
        l = 0
        r = len(cumulative)-1
        while l+1<r:
            mid = (l+r)/2
            #print l
            #print r
            if cumulative[mid]<seed: # seed needs to be smaller the desiring indexed value
                l = mid
            else:
                r = mid
        return r





    def generateDistribution(self, minSample, maxSample, numSamples, popularityList, logger):
        binCount = 4
        accessDict = self.readDistFromFile("SegmentPopularityTimeDistrib.dat")
        # print accessDict
        allsamples =  self.generateBinBasedDistribution(minSample, maxSample, numSamples, popularityList, binCount, accessDict, logger)
        print allsamples
        self.plotDistribution(allsamples, 'druid_distribution.png', 'Druid-Distribution')
        return allsamples




for i in xrange(1, 2):
    yahoo = Yahoo()
    yahoo.generateDistribution( 1, 100, 100, [], None)

