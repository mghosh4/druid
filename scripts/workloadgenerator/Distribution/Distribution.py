import numpy


class Uniform(object):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList):
        # start and end is inclusive
        return numpy.random.random_integers(minSample, maxSample, numSamples)

class Zipfian(object):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList):
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

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList):
        latestsample = super(Latest, self).generateDistribution(minSample, maxSample, numSamples, popularityList)
        return [maxSample - x + minSample for x in latestsample]


class ScrambledZipfian(Zipfian):

    def generateDistribution(self, minSample, maxSample, numSamples, popularityList):
        scrambledzipfiansample = super(ScrambledZipfian, self).generateDistribution(minSample, maxSample, numSamples, popularityList)
        itemcount = maxSample - minSample + 1
        return [minSample + x % itemcount for x in scrambledzipfiansample]
