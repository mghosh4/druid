#!/usr/bin/python
import os, sys
from pydruid.client import *
from datetime import datetime, timedelta
from pytz import *
import logging
import random
import Queue
import threading
import math
import time as tm

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

def applyOperation(query, config, logger):
    dbOpsHandler = AsyncDBOpsHandler(config)
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

def threadoperation(queryPerSec):
    @gen.coroutine
    def printresults():
        logger.info('{} {} {} {}'.format(start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S"), runtime, queryPerSec))
        line = list()
        querypermin = queryPerSec * 60
        endtime = datetime.now(timezone('UTC')) + timedelta(minutes=runtime)
        popularitylist = list()
        while True:
            time = datetime.now(timezone('UTC'))
            logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
            if time >= endtime:
                break

            #Query generated every minute. This is to optimize the overhead of query generation and also because segment granularity is minute
            newquerylist = list()
            if filename != "":
                newquerylist = QueryGenerator.generateQueriesFromFile(start, end, querypermin, timeAccessGenerator, periodAccessGenerator, filename)
            elif isbatch == True:
                newquerylist = QueryGenerator.generateQueries(start, end, querypermin, timeAccessGenerator, periodAccessGenerator, popularitylist)
            else:
                newquerylist = QueryGenerator.generateQueries(start, time, querypermin, timeAccessGenerator, periodAccessGenerator, popularitylist)

            for query in newquerylist:
                try:
                    line.append(applyOperation(query, config, logger))
                except Exception as inst:
                    logger.error(type(inst))     # the exception instance
                    logger.error(inst.args)      # arguments stored in .args
                    logger.error(inst)           # __str__ allows args to be printed directly
                    x, y = inst.args
                    logger.error('x =', x)
                    logger.error('y =', y)
        

            nextminute = time + timedelta(minutes=1)
            timediff = (nextminute - datetime.now(timezone('UTC'))).total_seconds()
            if timediff > 0:
                yield gen.sleep(timediff)

        wait_iterator = gen.WaitIterator(*line)
        while not wait_iterator.done():
            try:
                result = yield wait_iterator.next()
            except Exception as e:
                logger.error("Error {} from {}".format(e, wait_iterator.current_future))
            else:
                logger.info("Result {} received from {} at {}".format(
                    result, wait_iterator.current_future,
                    wait_iterator.current_index))
    
    IOLoop().run_sync(printresults)
    
## Main Code
logKey = 'workloadgen'
logfilename = 'workloadgenerator.log'
logformat = '%(asctime)s (%(threadName)-10s) %(message)s'

logger = logging.getLogger(logKey)
logger.setLevel(logging.DEBUG)
logger.propagate = False

fh = logging.FileHandler(logfilename, 'w')
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

formatter = logging.Formatter(logformat)
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

configFile = checkAndReturnArgs(sys.argv, logKey)
config = getConfig(configFile)

accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
opspersecond = config.getOpsPerSecond()
runtime = config.getRunTime() # in minutes
isbatch = config.getBatchExperiment()
filename = config.getFileName()

SINGLE_THREAD_THROUGHPUT = 4000

numthreads = int(math.ceil(float(opspersecond) / SINGLE_THREAD_THROUGHPUT))
lastthreadthroughput = opspersecond % SINGLE_THREAD_THROUGHPUT
timeAccessGenerator = DistributionFactory.createSegmentDistribution(accessdistribution)
periodAccessGenerator = DistributionFactory.createSegmentDistribution(perioddistribution)

newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
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

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=100)

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
logger.info("Total Number of Queries: " + str(totalqueries))
logger.info("Overall Throughput: " + str(throughput))
