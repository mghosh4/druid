from pydruid.client import *
from pylab import plt
from Query import Query

class DBOpsHandler:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def topn(self, query):
		#config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		tn = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations=config.getAggregations(), dimension=config.getDimension(), filter=config.getFilter(), metric=config.getMetric(), threshold=config.getThreshold())
		x = tn.export_pandas()
	def timeseries(self, query):
		#config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		ts = newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations=config.getAggregations(), post_aggregations=config.getPostAggregations(), filter=config.getFilter())
		x = ts.export_pandas()
	def groupby(self, query):
		#config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		gb = newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=config.getDimension(), aggregations=config.getAggregations(), filter=config.getFilter())
		x = gb.export_pandas()
