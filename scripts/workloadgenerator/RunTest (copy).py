#!/usr/bin/python
import os, sys
from datetime import datetime, timedelta
from pytz import *
import logging
import random
import Queue
import threading
from pydruid.client import *
#from Query import Query
from datetime import datetime, date
sys.path.append(os.path.abspath('Distribution'))
sys.path.append(os.path.abspath('Query'))
sys.path.append(os.path.abspath('Config'))
sys.path.append(os.path.abspath('DBOpsHandler'))

from ParseConfig import ParseConfig
from DBOpsHandler import DBOpsHandler
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory

#class FuncThread(threading.Thread):
#	def __init__(self, target, *args):
#		self._target = target
#		self._args = args
#		threading.Thread.__init__(self)
#	def run(self):
#		self._target(*self._args)

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


def randZipf(n, alpha, numSamples): 
		tmp = numpy.power( numpy.arange(1, n+1), -alpha )
		zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
		distMap = [x / zeta[-1] for x in zeta]
		u = numpy.random.random(numSamples)
		v = numpy.searchsorted(distMap, u)
		samples = [t-1 for t in v]
		print "sample"
		print samples
		return sampless
#def applyWorkload(self, numQueries, newquerylist = []):
#	for i in xrange(numQueries):
#		self.applyOperation(newquerylist[i])

def applyOperation(query, config, logger):
		dbOpsHandler = DBOpsHandler(config)
		if querytype == "timeseries":

			return dbOpsHandler.timeseries(query, logger)
		elif querytype == "topn":
			return dbOpsHandler.topn(query, logger)
		elif querytype == "groupby":
			return dbOpsHandler.groupby(query, logger)
		elif querytype == "segmentmetadata":
			return dbOpsHandler.segmentmetadata(query, logger)
		elif querytype == "timeboundary":
			return dbOpsHandler.timeboundary(query, logger)

def applyOperations(querylist, config, logger):
	for i in xrange(len(querylist)):
		applyOperation(querylist[i], config, logger)

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
numqueries = config.getNumQueries()
opspersecond = config.getOpsPerSecond()
queryruntime = config.getQueryRuntime()
numcores = config.getNumCores()
runtime = config.getRunTime()
filename = config.getfilename()

numthreads = int(opspersecond * queryruntime)
if(numthreads > numcores - 1):
	print >> sys.stderr, "Cannot achieve desired throughput."
  	sys.exit(1)

timeAccessGenerator = DistributionFactory.createSegmentDistribution(accessdistribution)

periodAccessGenerator = DistributionFactory.createSegmentDistribution(perioddistribution)

newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
#tb = newquery.time_boundary(datasource=config.getDataSource())
#edit by lexu
#time = datetime.now(timezone('UTC'))
#time = datetime(2016, 8, 23, 9, 3, 0 )
start = datetime(2016, 8, 23, 9, 0, 0 )

time_string = "2016-08-23T09:03:00.0000Z"
time = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S.%fZ")
start_string = "2016-08-23T09:00:00.0000Z"
start = datetime.strptime(start_string, "%Y-%m-%dT%H:%M:%S.%fZ")
time = utc.localize(time)
start = utc.localize(start)
#print (end_date-date).total_seconds()
#startdict = tb[0]
#start = startdict['result']['minTime']
#start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')
#edit by lexu
#start = "2016-08-23T09:00:00.0000Z"
#edit by lexu
#start = utc.localize(start)

minqueryperiod = 0
maxqueryperiod = time-start
x = maxqueryperiod.total_seconds()
maxqueryperiod = int(x)
#while time.time() < t_end:
	#newquerylist = QueryGenerator.generateQueries(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator);


#querylistsegment = len(newquerylist)/numthreads
#querylistsegmentremainder = len(newquerylist)%numthreads

#threadarray = []


def threadoperation(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator, config, logger, x, values):
	
	successfulquerytime = 0
	successfulquerycount = 0
	failedquerytime = 0
	failedquerycount = 0
	totalquerytime = 0
	totalquerycount = 0
	endtime = datetime.now() + timedelta(minutes=runtime)
	flag = 0
	print "hello again!"
	while flag < 3:
		flag = flag + 1
		if datetime.now() >= endtime:
			break
		time = datetime.now(timezone('UTC'))
		#newquerylist = QueryGenerator.generateQueries(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator);
        if(filename!=""):
        	print "generating requests from file!"
        	newquerylist = QueryGenerator.generateQueriesFromFile(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator, filename)
        else:
        	newquerylist = QueryGenerator.generateQueries(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator, currentSegRank)        
		
		print newquerylist
		line = applyOperation(newquerylist[0], config,logger)
        
		if ("Successful" in line[0]):
			print line[1].count
			successfulquerytime += float(line[0][11:])
			successfulquerycount += 1
			totalquerytime += float(line[0][11:])
			totalquerycount += 1
		else:
			failedquerytime += float(line[7:])
			failedquerycount += 1
			totalquerytime += float(line[7:])
			totalquerycount += 1

	datastructure = [successfulquerytime, successfulquerycount, failedquerytime, failedquerycount, totalquerytime, totalquerycount]
	values.put(datastructure)


print "hello!"
values = Queue.Queue(maxsize=0)
for i in xrange(numthreads):
	logger = logging.getLogger('thread-%s' % i)
	logger.setLevel(logging.DEBUG)

	file_handler = logging.FileHandler('thread-%s.log' % i)

	formatter = logging.Formatter('(%(threadName)-10s) %(message)s')
	file_handler.setFormatter(formatter)

	logger.addHandler(file_handler)
	delay = random.random()
	t = threading.Thread(target=threadoperation, args=(start, time, numqueries, timeAccessGenerator, minqueryperiod, maxqueryperiod, periodAccessGenerator, config, logger, i, values))
	t.start()

main_thread = threading.currentThread()
for t in threading.enumerate():
	if t is not main_thread:
		t.join()

#for i in xrange(numthreads):
#	f = open('thread-%s.log' % i, 'r')
#	for line in f:
#		print line[13:23]
#		if (line[13:23] == "Successful"):
#			successfulquerytime += float(line[25:])
#			successfulquerycount += 1
#			totalquerytime += float(line[25:])
#			totalquerycount += 1
#			currentmaxcompletiontime += float(line[25:])
#		else:
#			print line[20:]
#			failedquerytime += float(line[20:])
#			failedquerycount += 1
#			totalquerytime += float(line[20:])
#			totalquerycount += 1
#			currentmaxcompletiontime += float(line[20:])
#	if(currentmaxcompletiontime > maxcompletiontime):
#		maxcompletiontime = currentmaxcompletiontime
#	currentmaxcompletiontime = 0



#threadresults = []
#index = 0
#while(values.empty() == False):
#	threadresults.append(values.get())

#threadsuccessfulquerylatency = []
#threadfailedquerylatency = []
#threadtotalquerylatency = []
#maxcompletiontime = 0
#for i in xrange(threadresults):
#	threadsuccessfulquerylatency[i] = threadresults[i][0]/float(threadresults[i][1])
#	if(threadresults[i][3] != 0):
#		threadfailedquerylatency[i] = threadresults[i][2]/float(threadresults[i][3])
#	else:
#		threadfailedquerylatency[i] = 0
#	threadtotalquerylatency[i] = threadresults[i][4]/float(threadresults[i][5])
#	if(threadresults[i][4] >= maxcompletiontime):
#		maxcompletiontime = threadresults[i][4]
#successfulquerylatency = float(sum(threadsuccessfulquerylatency)/len(threadsuccessfulquerylatency))
#totalsuccessfulqueries = 0
#failedquerylatency = float(sum(threadfailedquerylatency)/len(threadfailedquerylatency))
#totalfailedqueries = 0
#totalquerylatency = float(sum(threadtotalquerylatency)/len(threadtotalquerylatency))
#totalqueries = 0
#for i in xrange(threadresults):
#	totalsuccessfulqueries += threadresults[i][1]
#	totalfailedqueries += threadresults[i][3]
#	totalqueries += threadresults[i][5]

#f = open('querymetrics.log', 'a')
#f.write("Total Completion Time : " + `maxcompletiontime` + "\n")
#f.write("Number of Successful Queries : " + `totalsuccessfulqueries` + "\n")
#f.write("Successful Query Latency : " + `successfulquerylatency` + "\n")
#f.write("Failed Query Latency : " + `failedquerylatency` + "\n")
#f.write("Number of Failed Queries : " + `totalfailedqueries` + "\n")
#f.write("Total Query Latency : " + `totalquerylatency` + "\n")
#f.write("Total Queries : " + `totalqueries` + "\n")

#for i in xrange(numthreads):
#	try:
		#thread.start_new_thread(applyOperations, (newquerylist[i:i+querylistsegment], config))
#		threadarray.append(FuncThread(applyOperations, newquerylist[i:i+querylistsegment], config))
#		threadarray[i].start()
#	except:
#		print "Error: unable to start thread"

#for i in xrange(numthreads):
#	threadarray[i].join()