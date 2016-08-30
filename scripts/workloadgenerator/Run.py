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
sys.path.append(os.path.abspath('External'))
sys.path.append(os.path.abspath('Utils'))
from ParseConfig import ParseConfig
from AsyncDBOpsHandler import AsyncDBOpsHandler
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

def threadoperation(dataStartTime, dataEndTime, runTime, isbatch, queryPerSec, timeAccessGenerator, periodAccessGenerator, config, logger):
    @gen.coroutine
    def printresults():
        logger.info('{} {} {} {}'.format(dataStartTime.strftime("%Y-%m-%d %H:%M:%S"), dataEndTime.strftime("%Y-%m-%d %H:%M:%S"), runTime, queryPerSec))
        line = list()
        querypermin = queryPerSec * 60
        endtime = datetime.now(timezone('UTC')) + timedelta(minutes=runtime)
        while True:
            time = datetime.now(timezone('UTC'))
            logger.info("Time: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
            if time >= endtime:
                break

            #Query generated every minute. This is to optimize the overhead of query generation and also because segment granularity is minute
            newquerylist = list()
            if isbatch == True:
                newquerylist = QueryGenerator.generateQueries(dataStartTime, dataEndTime, querypermin, timeAccessGenerator, periodAccessGenerator)
            else:
                newquerylist = QueryGenerator.generateQueries(dataStartTime, time, querypermin, timeAccessGenerator, periodAccessGenerator)

            for query in newquerylist:
                try:
                    line.append(applyOperation(query, config, logger))
                except Exception as inst:
                    logger.info(type(inst))     # the exception instance
                    logger.info(inst.args)      # arguments stored in .args
                    logger.info(inst)           # __str__ allows args to be printed directly
                    x, y = inst.args
                    logger.info('x =', x)
                    logger.info('y =', y)
        

            nextminute = time + timedelta(minutes=1)
            timediff = (nextminute - datetime.now(timezone('UTC'))).total_seconds()
            if timediff > 0:
                tm.sleep(timediff)

        wait_iterator = gen.WaitIterator(*line)
        while not wait_iterator.done():
            try:
                result = yield wait_iterator.next()
            except Exception as e:
                logger.info("Error {} from {}".format(e, wait_iterator.current_future))
            else:
                logger.info("Result {} received from {} at {}".format(
                    result, wait_iterator.current_future,
                    wait_iterator.current_index))
    
    IOLoop().run_sync(printresults)
    
## Main Code
configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

accessdistribution = config.getAccessDistribution()
perioddistribution = config.getPeriodDistribution()
querytype = config.getQueryType()
opspersecond = config.getOpsPerSecond()
runtime = config.getRunTime() # in minutes
isbatch = config.getBatchExperiment()

SINGLE_THREAD_THROUGHPUT = 4000
values = Queue.Queue(maxsize=0)

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

logging_config = dict(
    version = 1,
    formatters = {
        'f': {'format':
              '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
        },
    handlers = {
        'h': {'class': 'logging.StreamHandler',
              'formatter': 'f',
              'level': logging.DEBUG}
        },
    loggers = {
        'tornado.general': {'handlers': ['h'],
                 'level': logging.DEBUG}
        }
)

dictConfig(logging_config)

AsyncHTTPClient.configure(None, max_clients=100)

for i in xrange(numthreads):
    logger = logging.getLogger('thread-%s' % i)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('thread-%s.log' % i)

    formatter = logging.Formatter('(%(threadName)-10s) %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    delay = random.random()
    time = datetime.now(timezone('UTC'))
    numqueries = SINGLE_THREAD_THROUGHPUT
    if i == numthreads - 1:
        numqueries = lastthreadthroughput
    t = threading.Thread(target=threadoperation, args=(start, end, runtime, isbatch, numqueries, timeAccessGenerator, periodAccessGenerator, config, logger))
    t.start()

main_thread = threading.currentThread()
for t in threading.enumerate():
    if t is not main_thread:
        t.join()
