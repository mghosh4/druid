#!/usr/bin/python
import os, sys
import datetime
sys.path.append(os.path.abspath('Distribution'))
sys.path.append(os.path.abspath('Query'))
sys.path.append(os.path.abspath('Config'))
sys.path.append(os.path.abspath('DBOpsHandler'))

from pydruid.client import *
from pylab import plt

from ParseConfig import ParseConfig
from DBOpsHandler import DBOpsHandler
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory

def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	requiredNumOfArgs = 2
	if len(args) < requiredNumOfArgs:
		print "Usage: python " + args[0] + " <config_file>"
		exit()

	configFile = getConfigFile(args)
	return configFile

def getConfigFilePath(configFile):
	return os.path.abspath("../Configs/" + configFile) 

def getConfig(configFile):
	configFilePath = configFile
	return ParseConfig(configFilePath)

def applyWorkload(self, numQueries, newquerylist = []):
	for i in xrange(numQueries):
		self.applyOperation(newquerylist[i])

def applyOperation(self, query):
		dbOpsHandler = DBOpsHandler(config)
		if querytype == "ts":
			dbOpsHandler.timeseries(query)
		elif querytype == "tn":
			dbOpsHandler.topn(query)
		elif querytype == "gb":
			dbOpsHandler.groupby(query)

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
minqueryperiod = config.getMinQueryPeriod()
maxqueryperiod = config.getMaxQueryPeriod()
numqueries = config.getNumQueries()
earliestyear = config.getEarliestYear()
earliestmonth = config.getEarliestMonth()
earliestday = config.getEarliestDay()
earliesthour = config.getEarliestHour()
earliestminute = config.getEarliestMinute()
earliestsecond = config.getEarliestSecond()

timeAccessGenerator = DistributionFactory.createSegmentDistribution(accessdistribution)

periodAccessGenerator = DistributionFactory.createSegmentDistribution(perioddistribution)

#time = calendar.timegm(time.strptime('2008 320 14:17:15', '%Y %j %H:%M:%S'))

time = datetime.datetime.now()

start = datetime.datetime(earliestyear, earliestmonth, earliestday, earliesthour, earliestminute, earliestsecond)

newquerylist = QueryGenerator.generateQueries(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator);
applyWorkload(numqueries, newquerylist)



