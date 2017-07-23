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
        self.newquery = AsyncPyDruid(brokerNameUrl, self.config.getBrokerEndpoint())
        self.logger = logger

    @gen.coroutine
    def segmentmetadata(self, query):
        sm = yield self.newquery.segment_metadata(datasource=self.config.getDataSource(), intervals=query.interval)
        raise gen.Return(sm.export_pandas())

    @gen.coroutine
    def timeboundary(self, query):
        tb = yield self.newquery.time_boundary(datasource=self.config.getDataSource())
        raise gen.Return(tb.export_pandas())

    @gen.coroutine
    def topn(self, query):
        self.logger.info("Query id "+str(query.getID())+",interval "+str(query.getInterval()))
        tn = yield self.newquery.topn(datasource=self.config.getDataSource(), granularity=self.config.getGranularity(), intervals=query.interval, aggregations={"count": aggregators.doublesum("count")}, dimension=self.config.getDimension().split(",")[0], metric=self.config.getMetric(), threshold=self.config.getThreshold())
        self.processQueryCompletion(query)
        raise gen.Return(tn.export_pandas())

    def processQueryCompletion(self, query):
        query.setRxTime(datetime.now())
        self.logger.info("Query id "+str(query.getID())+",time "+str(query.getExecutionTime()))

#FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def timeseries(self, query):
        self.logger.info("Query id "+str(query.getID())+",interval "+str(query.getInterval()))
        ts = yield self.newquery.timeseries(datasource=self.config.getDataSource(), granularity=self.config.getGranularity(), intervals=query.interval, aggregations={"count": aggregators.doublesum("count")})
        self.processQueryCompletion(query)
        raise gen.Return(ts.export_pandas())

#FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def groupby(self, query):
        self.logger.info("Query id "+str(query.getID())+",interval "+str(query.getInterval()))
        gb = yield self.newquery.groupby(datasource=self.config.getDataSource(), granularity=self.config.getGranularity(), intervals=query.interval, dimensions=(self.config.getDimension()).split(","), aggregations={"count": aggregators.doublesum("count")})
        self.processQueryCompletion(query)
        raise gen.Return(gb.export_pandas())
