from datetime import datetime, date
from pydruid.async_client import *
import os, sys
import ast
import logging
from pydruid.utils import *
from tornado import gen

from Query import Query

class AsyncDBOpsHandler:

    def __init__(self, config, brokerNameUrl, logger):
        self.config = config
        self.brokerNameUrl = brokerNameUrl
	self.logger = logger

    def getConfig(self):
        return self.config

    def getAsyncPyDruid(self):
        config = self.getConfig()
        newquery = AsyncPyDruid(self.brokerNameUrl, config.getBrokerEndpoint())
        return newquery

    @gen.coroutine
    def segmentmetadata(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        sm = yield newquery.segment_metadata(datasource=config.getDataSource(), intervals=query.interval)
        raise gen.Return(sm.export_pandas())

    @gen.coroutine
    def timeboundary(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        tb = yield newquery.time_boundary(datasource=config.getDataSource())
        raise gen.Return(tb.export_pandas())

    @gen.coroutine
    def topn(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        tn = yield newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={"count": aggregators.doublesum("count")}, dimension=config.getDimension().split(",")[0], metric=config.getMetric(), threshold=config.getThreshold())
        raise gen.Return(tn.export_pandas())

    def processQueryCompletion(self, query):
	query.setRxTime(datetime.now())
        self.logger.info("Query id "+str(query.getID())+",interval "+str(query.getInterval())+",time "+str(query.getExecutionTime()))

    #FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def timeseries(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        self.logger.info("Query id "+str(query.getID())+",interval "+str(query.getInterval()))
        ts = yield newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={"count": aggregators.doublesum("count")})
	#self.processQueryCompletion(query)
        raise gen.Return(ts.export_pandas())
    
    #FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def groupby(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        gb = yield newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=(config.getDimension()).split(","), aggregations={"count": aggregators.doublesum("count")})
        raise gen.Return(gb.export_pandas())
