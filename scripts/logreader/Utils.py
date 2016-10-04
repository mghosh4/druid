import os
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
        
    @staticmethod
    def percentile99(l):
        return numpy.percentile(l, 99)
        
    @staticmethod
    def percentile90(l):
        return numpy.percentile(l, 90)
        
    @staticmethod
    def percentile75(l):
        return numpy.percentile(l, 75)                