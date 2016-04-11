from pydruid.client import *
from pylab import plt
from Query import Query

class DBOpsHandler:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def topn(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		tn = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')}, dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
		x = newquery.export_pandas()
		print x

	def timeseries(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		postaggregatearg = ""
		filterarg = ""

		if(config.getPostAggregations() != ""):
			postaggregatearg = ", post_aggregations=" + config.getPostAggregations()

		if(config.getFilter() != ""):
			filterarg = ", filter=" + config.getFilter()
		argstring = "datasource=" + config.getDataSource()  + ", granularity="+ config.getGranularity() + ", intervals=" + query.interval + ", aggregations=" + config.getAggregations() + postaggregatearg + filterarg
		args = dict(tuple(e.split('=')) for e in argstring.split(', '))
		ts = newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')})
		x = newquery.export_pandas()
		print x

	def groupby(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		gb = newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=["name", "value"], aggregations={"count": doublesum("count")})
		x = newquery.export_pandas()
		print x
