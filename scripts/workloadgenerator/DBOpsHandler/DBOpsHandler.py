from pydruid.client import *
#from pylab import plt
from Query import Query
from datetime import datetime, date

class DBOpsHandler:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def segmentmetadata(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		sm = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')}, dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()

		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			print x
			print t2
		else:
			print "Query Failed"

	def timeboundary(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		tb = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')}, dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()

		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			print x
			print t2
		else:
			print "Query Failed"

	def topn(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		tn = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')}, dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()

		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			print x
			print t2
		else:
			print "Query Failed"

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
		t1 = datetime.now().time()
		ts = newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={'count': doublesum('count')})
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()
		
		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			print x
			print t2
		else:
			print "Query Failed"

	def groupby(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		gb = newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=["name", "value"], aggregations={"count": doublesum("count")})
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()
		
		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			print x
			print t2
		else:
			print "Query Failed"
