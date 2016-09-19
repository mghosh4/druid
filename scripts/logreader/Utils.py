import os

class Utils(object):

    @staticmethod
    def writeTimeSeriesData(filename, timeseriesdata):
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
            
        f = open(filename, 'w')
        for key,value in timeseriesdata.iteritems():
            f.write(key + "\t" + str(value) + "\n")
        f.close()