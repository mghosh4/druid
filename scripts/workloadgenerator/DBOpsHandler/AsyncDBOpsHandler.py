from datetime import datetime, date
import os, sys
import ast
import logging
from pydruid.utils import *
from tornado import gen

from async_client import *
from Query import Query

class AsyncDBOpsHandler:

    def __init__(self, config):
        self.config = config

    def getConfig(self):
        return self.config

    def getAsyncPyDruid(self):
        config = self.getConfig()
        newquery = AsyncPyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
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
        tn = yield newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations=ast.literal_eval(config.getAggregations), dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
        raise gen.Return(tn.export_pandas())

    #FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def timeseries(self, query, logger):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        ts = yield newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={"count": aggregators.doublesum("count")})
        raise gen.Return(ts.export_pandas())
    
    #FILTER AND POST_AGGREGATIONS ARE OPTIONAL
    @gen.coroutine
    def groupby(self, query):
        config = self.getConfig()
        newquery = self.getAsyncPyDruid()
        gb = yield newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=(config.getDimension()).split(","), aggregations=ast.literal_eval(config.getAggregations))
        raise gen.Return(gb.export_pandas())
