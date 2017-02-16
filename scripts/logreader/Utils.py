import os,sys
import numpy

class Utils(object):

    @staticmethod
    def writeTimeSeriesData(filename, timeseriesdata):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
            
        f = open(filename, 'w')
        for key,value in timeseriesdata.iteritems():
            f.write(str(key) + "\t" + str(value) + "\n")
        f.close()
    
    @staticmethod    
    def writeTimeSeriesMetricStats(filename, metricstats, stats, headerString):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
            
        f = open(filename, 'w')
        f.write(headerString)
        for key in metricstats[stats[0]].keys():
            statsString = ""
            for stat in stats:
                statsString = statsString + "\t" + str(metricstats[stat][key])
            f.write(str(key) + statsString + "\n")
        f.close()
        
    @staticmethod    
    def writeOverallMetricStats(filename, metricstats, stats, headerString):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        f = open(filename, 'w')
        f.write(headerString)
        statsString = ""
        for stat in stats:
            statsString = statsString + "\t" + str(metricstats[stat])
        f.write(statsString + "\n")
        f.close()

    # Code from internship
    @staticmethod
    def createCDF(values):
        sortedList = sorted(values)
        valueHist = dict()

        try:
            for val in values:
                roundedVal = float(int(val * 100)) / 100
                if roundedVal not in valueHist:
                    valueHist[roundedVal] = 1
                else:
                    valueHist[roundedVal] += 1
        except:
            pass

        latencyHistCum = []
        total = 0
        for val in sorted(valueHist.keys()):
            total += valueHist[val]
            latencyHistCum.append((val, total))

        valueCDF = []
        for (val, num) in latencyHistCum:
            valueCDF.append((val, float(num) / total))

        return valueCDF

    @staticmethod
    def writeToFile(filename, data):
        if not os.path.exists(os.path.dirname(filename)):
	    print "filename 2: " + filename
            os.makedirs(os.path.dirname(filename))

        file = open(filename, "w")
        for (k, v) in data:
            file.write(str(k))
            file.write(' ' + str(v) + '\n')

    @staticmethod
    def writeCDF(filename, values):
        valueCDF = Utils.createCDF(values)
        Utils.writeToFile(filename, valueCDF)
        
    @staticmethod
    def percentile95(l):
        return numpy.percentile(l, 95)
        
    @staticmethod
    def percentile90(l):
        return numpy.percentile(l, 90)
        
    @staticmethod
    def percentile75(l):
        return numpy.percentile(l, 75)                
