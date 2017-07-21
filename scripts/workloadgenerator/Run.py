#!/usr/bin/python

import os, sys
from pydruid.client import *
from datetime import datetime, timedelta
from pytz import *
import logging
import numpy
import socket
#import random
import Queue
import threading
import math
import time as tm
import signal

from timeit import default_timer as timer
from datetime import datetime, date
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from logging.config import dictConfig

sys.path.append(os.path.abspath('Distribution'))
sys.path.append(os.path.abspath('Query'))
sys.path.append(os.path.abspath('Config'))
sys.path.append(os.path.abspath('DBOpsHandler'))
sys.path.append(os.path.abspath('Utils'))
from ParseConfig import ParseConfig
from AsyncDBOpsHandler import AsyncDBOpsHandler
from DBOpsHandler import DBOpsHandler
from QueryGenerator import QueryGenerator
from DistributionFactory import DistributionFactory

def getConfigFile(args):
    return args[1]

def checkAndReturnArgs(args, logKey):
    logger = logging.getLogger(logKey)
    requiredNumOfArgs = 2
    if len(args) < requiredNumOfArgs:
        logger.error("Usage: python " + args[0] + " <config_file>")
        exit()

    configFile = getConfigFile(args)
    return configFile

def getConfigFilePath(configFile):
    return os.path.abspath("../Configs/" + configFile) 

def getConfig(configFile):
    configFilePath = configFile
    return ParseConfig(configFilePath)

def signal_term_handler(signum, frame):
    print signum, frame
    print "print stack frames:"
    traceback.print_stack(frame)
    sys.exit(0)

def applyOperation(query, config, brokerNameUrl, logger):
    dbOpsHandler = AsyncDBOpsHandler(config, brokerNameUrl, logger)
    if querytype == "timeseries":
        return dbOpsHandler.timeseries(query)
    elif querytype == "topn":
        return dbOpsHandler.topn(query)
    elif querytype == "groupby":
        return dbOpsHandler.groupby(query)
    elif querytype == "segmentmetadata":
        return dbOpsHandler.segmentmetadata(query)
    elif querytype == "timeboundary":
        return dbOpsHandler.timeboundary(query)
    elif querytype == "mixture":
        randomNumber = numpy.random.randint(0, 100);
        qtype = 0
        for queryweight in sorted(queryratio):
            if randomNumber <= queryweight:
                break
            qtype = qtype + 1
            randomNumber = randomNumber - queryweight

        if qtype == 2:
            return dbOpsHandler.timeseries(query)
        elif qtype == 1:
            return dbOpsHandler.topn(query)
        elif qtype == 0:
            return dbOpsHandler.groupby(query)

# def threadoperation(queryPerSec):
#     @gen.coroutine
#     def printresults():
#         logger.info('{} {} {} {}'.format(start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), runtime, queryPerSec))
#         line = list()
#         querypermin = queryPerSec * 60
#         endtime = datetime.now(timezone('UTC')) + timedelta(minutes=runtime)
#         popularitylist = list()
#         newquerylist = list()
#         if filename != "":
#             newquerylist = QueryGenerator.generateQueriesFromFile(start, end, querypermin * runtime, timeAccessGenerator, periodAccessGenerator, filename)
#         elif isbatch == True:
#             newquerylist = QueryGenerator.generateQueries(start, end, querypermin * runtime, timeAccessGenerator, periodAccessGenerator, popularitylist)
#         if filename != "" or isbatch == True:
#             count = 0
#             time = datetime.now(timezone('UTC'))
#             logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
#             nextminute = time + timedelta(minutes=1)
#             for query in newquerylist:
#                 try:
#                     line.append(applyOperation(query, config, brokernameurl, logger))
#                 except Exception as inst:
#                     logger.error(type(inst))     # the exception instance
#                     logger.error(inst.args)      # arguments stored in .args
#                     logger.error(inst)           # __str__ allows args to be printed directly
#                     x, y = inst.args
#                     logger.error('x =', x)
#                     logger.error('y =', y)

#                 count = count + 1
#                 if count >= querypermin:
#                     timediff = (nextminute - datetime.now(timezone('UTC'))).total_seconds()
#                     if timediff > 0:
#                         yield gen.sleep(timediff)
#                     count = 0
#                     time = datetime.now(timezone('UTC'))
#                     logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
#                     nextminute = time + timedelta(minutes=1)

#         else:
#             queryendtime = start
#             nextminute = start
#             endtime = start + timedelta(minutes=runtime)
#             while True:
#                 #time = datetime.now(timezone('UTC'))
#                 logger.info("Time: {}".format(nextminute.strftime("%Y-%m-%d %H:%M:%S")))
#                 querytime = nextminute - timedelta(minutes=5)
#                 if querytime >= endtime:
#                     break

#                 elapsedtimeinmins = (querytime - queryendtime).total_seconds() / 60
#                 if elapsedtimeinmins >= 5:
#                     queryendtime = querytime

#                 querystarttime = datetime.now(timezone('UTC'))
#                 if start < queryendtime:
#                     logger.info("Run.py start queryendtime "+str(start)+", "+str(queryendtime))
#                     #Query generated every minute. This is to optimize the overhead of query generation and also because segment granularity is minute
#                     newquerylist = QueryGenerator.generateQueries(start, queryendtime, querypermin, timeAccessGenerator, periodAccessGenerator, popularitylist, logger)
    
#                     for query in newquerylist:
#                         try:
#                             line.append(applyOperation(query, config, brokernameurl, logger))
#                         except Exception as inst:
#                             logger.error(type(inst))     # the exception instance
#                             logger.error(inst.args)      # arguments stored in .args
#                             logger.error(inst)           # __str__ allows args to be printed directly
#                             x, y = inst.args
#                             logger.error('x =', x)
#                             logger.error('y =', y)

#                 nextminute = nextminute + timedelta(minutes=1)
#                 timediff = (datetime.now(timezone('UTC')) - querystarttime).total_seconds()
#                 if timediff < 60:
#                     yield gen.sleep(60-timediff)
    
#         wait_iterator = gen.WaitIterator(*line)
#         while not wait_iterator.done():
#             try:
#                 result = yield wait_iterator.next()
#             except Exception as e:
#                 logger.error("Error {} from {}".format(e, wait_iterator.current_future))
#             #else:
#             #    logger.info("Result {} received from {} at {}".format(
#             #        result, wait_iterator.current_future,
#             #        wait_iterator.current_index))
    
#     IOLoop().run_sync(printresults)

# generates tuples of (num queries, time to sleep) as per poisson distribution
def genPoissonQuerySchedule(queryPerMilliSecond, numSamples):
    
    numSamples = int(1.25*numSamples) # generate some additional samples in case the query count falls short
    samples = numpy.random.poisson(queryPerMilliSecond, numSamples)

    numQueries = 0
    querySchedule = []
    count = 0;

    for i in range(0, len(samples)):
        sample = samples[i]
        if(sample != 0):
            if(count != 0):
                querySchedule.append([0, count])
                count = 0
            querySchedule.append([sample, 1])
            numQueries = numQueries + sample
        else:
            count = count + 1
    if(count != 0):
        querySchedule.append([0, count])

    return numQueries, querySchedule


def threadoperation(queryPerSec):
    @gen.coroutine
    def printresults():
        logger.info('{} {} {} {} {}'.format(start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), runtime, queryPerSec, segmentpopularityinterval))
        
        querypermin = queryPerSec * 60
        endtime = datetime.now(timezone('UTC')) + timedelta(minutes=runtime)
        line = list()
        popularitylist = list()
        newquerylist = list()
        
        if filename != "":
            newquerylist = QueryGenerator.generateQueriesFromFile(start, end, querypermin * runtime, timeAccessGenerator, periodAccessGenerator, filename)
        elif isbatch == True:
            newquerylist = QueryGenerator.generateQueries(start, end, querypermin * runtime, timeAccessGenerator, periodAccessGenerator, popularitylist)
        else:
            #logger.info("Run.py start queryendtime "+str(start)+", "+str(endtime))
            queryStartInterval = start
            queryEndInterval = start + timedelta(minutes=segmentpopularityinterval)
            for i in range(0, (runtime-segmentpopularityinterval)/segmentpopularityinterval):
                logger.info("Start generating queries for interval "+str(queryStartInterval)+" - "+str(queryEndInterval))
                newquerylist.extend(QueryGenerator.generateQueries(queryStartInterval, queryEndInterval, segmentpopularityinterval*querypermin, timeAccessGenerator, periodAccessGenerator, popularitylist, logger))
                queryEndInterval = queryEndInterval + timedelta(minutes=segmentpopularityinterval)

            if(runtime%segmentpopularityinterval != 0):
                newquerylist.extend(QueryGenerator.generateQueries(queryStartInterval, queryEndInterval, runtime%segmentpopularityinterval*querypermin, timeAccessGenerator, periodAccessGenerator, popularitylist, logger))
            logger.info("Finished generating queries. num queries generated "+str(len(newquerylist)))    
        
        if filename != "" or isbatch == True:
            count = 0
            time = datetime.now(timezone('UTC'))
            logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
            nextminute = time + timedelta(minutes=1)
            for query in newquerylist:
                try:
                    line.append(applyOperation(query, config, brokernameurl, logger))
                except Exception as inst:
                    logger.error(type(inst))     # the exception instance
                    logger.error(inst.args)      # arguments stored in .args
                    logger.error(inst)           # __str__ allows args to be printed directly
                    x, y = inst.args
                    logger.error('x =', x)
                    logger.error('y =', y)

                count = count + 1
                if count >= querypermin:
                    timediff = (nextminute - datetime.now(timezone('UTC'))).total_seconds()
                    if timediff > 0:
                        yield gen.sleep(timediff)
                    count = 0
                    time = datetime.now(timezone('UTC'))
                    logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
                    nextminute = time + timedelta(minutes=1)
        else:
            # frequency of queries per millisecond
            queryPerMilliSecond = float(queryPerSec)/1000;
            # number of samples spaced by 1 millisecond
            numSamples = (runtime - segmentpopularityinterval)*60*1000
            numQueries, querySchedule = genPoissonQuerySchedule(queryPerMilliSecond, numSamples)
            logger.info("Poisson numQueries = "+str(numQueries))

            # sleep initially till the segmentpopularityinterval
            yield gen.sleep(60)

            queryScheduleIdx = 0
            count = 0
            while count < len(newquerylist):
                sample = querySchedule[queryScheduleIdx]
                #logger.info("Poisson sample is "+str(sample[0])+", "+str(sample[1]))
                if(sample[0] == 0):
                    #logger.info("Sleeping for "+str(sample[1]))
                    yield gen.sleep(float(sample[1])/1000) # divide by 1000 to convert it into seconds
                else:
                    for i in range(0,sample[0]):
                        try:
                            line.append(applyOperation(newquerylist[count], config, brokernameurl, logger))
                            #applyOperation(newquerylist[count], config, brokernameurl, logger)
                            newquerylist[count].setTxTime(datetime.now())
                            #logger.info("Running query "+str(sample[0]))
                        except Exception as inst:
                            logger.error(type(inst))     # the exception instance
                            logger.error(inst.args)      # arguments stored in .args
                            logger.error(inst)           # __str__ allows args to be printed directly
                            x, y = inst.args
                            logger.error('x =', x)
                            logger.error('y =', y)
                        count = count + 1
                queryScheduleIdx = queryScheduleIdx + 1
    
        wait_iterator = gen.WaitIterator(*line)
        while not wait_iterator.done():
            try:
                result = yield wait_iterator.next()
            except Exception as e:
                logger.error("Error {} from {}".format(e, wait_iterator.current_future))
            #else:
            #    logger.info("Result {} received from {} at {}".format(
            #        result, wait_iterator.current_future,
            #        wait_iterator.current_index))
    
    IOLoop().run_sync(printresults)

    
## Main Code
numpy.random.seed(int(socket.gethostname().split(".")[0].split("-")[-1]))
signal.signal(signal.SIGTERM, signal_term_handler)
configFile = checkAndReturnArgs(sys.argv, '')
config = getConfig(configFile)


accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
queryratio = list()
if querytype == "mixture":
    queryratio = [int(n) for n in config.getQueryRatio().split(":")]
logfolder = config.getLogFolder()
opspersecond = config.getOpsPerSecond()
runtime = config.getRunTime() # in minutes
segmentpopularityinterval = config.getSegmentPopularityInterval() # in minutes
isbatch = config.getBatchExperiment()
filename = config.getFileName()
brokernameurl = config.getBrokerNodeUrl()
outputfilename = 'workloadgenerator.log'

if len(sys.argv) == 4:
    outputfilename = sys.argv[3]

if len(sys.argv) >= 3:
    brokernameurl = sys.argv[2]

print(outputfilename, brokernameurl)

logKey = 'workloadgen'
logfilename = logfolder + '/' + outputfilename
logformat = '%(asctime)s (%(threadName)-10s) %(message)s'

logger = logging.getLogger(logKey)
logger.setLevel(logging.DEBUG)
logger.propagate = False
curllogger = logging.getLogger('tornado.curl_httpclient')
curllogger.setLevel(logging.DEBUG)
curllogger.propagate = False

fh = logging.FileHandler(logfilename, 'w')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter(logformat)
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)
curllogger.addHandler(fh)
curllogger.addHandler(ch)

SINGLE_THREAD_THROUGHPUT = 400
if filename != "" or isbatch == True:
    SINGLE_THREAD_THROUGHPUT = 2000

numthreads = int(math.ceil(float(opspersecond) / SINGLE_THREAD_THROUGHPUT))
lastthreadthroughput = opspersecond % SINGLE_THREAD_THROUGHPUT
timeAccessGenerator = DistributionFactory.createSegmentDistribution(accessdistribution)
periodAccessGenerator = DistributionFactory.createSegmentDistribution(perioddistribution)

newquery = PyDruid(brokernameurl, config.getBrokerEndpoint())
tb = newquery.time_boundary(datasource=config.getDataSource())

startdict = tb[0]
start = startdict['result']['minTime']
start = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S.%fZ')
start = utc.localize(start)
end = startdict['result']['maxTime']
end = datetime.strptime(end, '%Y-%m-%dT%H:%M:%S.%fZ')
end = utc.localize(end)

minqueryperiod = 0
maxqueryperiod = int((end - start).total_seconds())

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=(25), defaults=dict(request_timeout=60))

t1 = datetime.now()
for i in xrange(numthreads): 
    time = datetime.now(timezone('UTC'))
    numqueries = SINGLE_THREAD_THROUGHPUT
    if i == numthreads - 1:
        if lastthreadthroughput == 0:
            numqueries = SINGLE_THREAD_THROUGHPUT
        else:
            numqueries = lastthreadthroughput
    t = threading.Thread(target=threadoperation, args=(numqueries,))
    t.start()

main_thread = threading.currentThread()
for t in threading.enumerate():
    if t is not main_thread:
        t.join()

totaltime = (datetime.now() - t1).total_seconds()
totalqueries = opspersecond * 60 * runtime
throughput = float(totalqueries) / totaltime

logger.info("Total Time Taken: " + str(totaltime))
