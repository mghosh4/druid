from pydruid.client import *
from Query import Query
from datetime import datetime, date
import ast
import logging
from pydruid.utils import *

class DBOpsHandler:

	def __init__(self, config):
		self.config = config

	def getConfig(self):
		return self.config

	def segmentmetadata(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		sm = newquery.segment_metadata(datasource=config.getDataSource(), intervals=query.interval)
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		#x = newquery.export_pandas()

		#print json.dumps(newquery.query_dict, indent=2)
		#if(x is not None):
		#	print x
		print t2
		#else:
		#	print "Query Failed"

	def timeboundary(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		tb = newquery.time_boundary(datasource=config.getDataSource())
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		#x = newquery.export_pandas()

		#print json.dumps(newquery.query_dict, indent=2)
		#if(x is not None):
		#	print x
		print t2
		#else:
		#	print "Query Failed"


			#FILTER AND POST_AGGREGATIONS ARE OPTIONAL
	def topn(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		tn = newquery.topn(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations=ast.literal_eval(config.getAggregations), dimension=config.getDimension(), metric=config.getMetric(), threshold=config.getThreshold())
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()

		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			#print x
			print "Succesful:" + t2
		else:
			print "Failed:" + t2


			#FILTER AND POST_AGGREGATIONS ARE OPTIONAL
	def timeseries(self, query, logger):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		ts = newquery.timeseries(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, aggregations={"count": doublesum("count")})#aggregations=ast.literal_eval(config.getAggregations))
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()
		
		#print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			#print x
			message = "Successful:" + `t2.total_seconds()`
			logger.info("Successful:" + `t2.total_seconds()`)
			return [message,x]
		else:
			message = "Failed:" + `t2.total_seconds()`
			logger.info("Failed:" + `t2.total_seconds()`)
			print message
			return message


			#FILTER AND POST_AGGREGATIONS ARE OPTIONAL
	def groupby(self, query):
		config = self.getConfig()
		newquery = PyDruid(config.getBrokerNodeUrl(), config.getBrokerEndpoint())
		t1 = datetime.now().time()
		gb = newquery.groupby(datasource=config.getDataSource(), granularity=config.getGranularity(), intervals=query.interval, dimensions=(config.getDimension()).split(","), aggregations=ast.literal_eval(config.getAggregations))
		t2 = datetime.combine(date.today(),datetime.now().time()) - datetime.combine(datetime.today(),t1)
		x = newquery.export_pandas()
		
		print json.dumps(newquery.query_dict, indent=2)
		if(x is not None):
			#print x
			print "Succesful:" + t2
		else:
			print "Failed:" + t2
