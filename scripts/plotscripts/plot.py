import os,sys
import numpy as numpy
import matplotlib.pyplot as plt
import csv

from PlotParseConfig import ParseConfig

def getConfigFile(args):
	return args[1]

def checkAndReturnArgs(args):
	required = 2
	if len(args) < required:
		print "Usage: python " + args[0] + " <config_file>"
		exit()
	configFile = getConfigFile(args)
	return configFile

def getConfig(configFile):
	configFilePath = configFile
	return ParseConfig(configFilePath)

configFile = checkAndReturnArgs(sys.argv)
config = getConfig(configFile)

historicalmetrics = config.getHistoricalMetrics()
coordinatormetrics = config.getCoordinatorMetrics()
brokermetrics = config.getBrokerMetrics()
throughputgraph = config.getThroughputGraph()
pathtoresults = config.getPathToResults()
numberofhistorical = config.getNumberOfHistorical()
numberofbroker = config.getNumberOfBroker()
numberofcoordinator = config.getNumberOfCoordinator()
distributions = config.getDistributions()
druidversion = config.getDruidVersion()

count = 1
for i in xrange(len(historicalmetrics)):
	fig = plt.figure(count)
	fig.suptitle(historicalmetrics[i])
	if(historicalmetrics[i] == "segment-count" or historicalmetrics[i] == "sys-mem-used"):
		for j in xrange(len(distributions)):
			for k in xrange(len(druidversion)):
				filename = pathtoresults[0] + "/result-" + distributions[j] + "-" + druidversion[k] + "/historical-" + historicalmetrics[i] + ".log"
				a = []
				b = []
				with open(filename) as csvfile:
					plots = csv.reader(csvfile, delimiter='	')
					plots.next()
					count = 1
					for row in plots:
						a.append(count)
						b.append(float(row[1]))
						count += 1
				label = distributions[j] + "-" + druidversion[k]
				plt.plot(a, b, label=label)
	else:
		for x in xrange(int(numberofhistorical[0])):
			for j in xrange(len(distributions)):
				for k in xrange(len(druidversion)):
					filename = pathtoresults[0] + "/result-" + distributions[j] + "-" + druidversion[k] + "/historical-" + str(x) + "-" + historicalmetrics[i] + ".log"
					a = []
					b = []
					with open(filename) as csvfile:
						plots = csv.reader(csvfile, delimiter='	')
						plots.next()
						count = 1
						for row in plots:
							a.append(count)
							b.append(float(row[1]))
							count += 1
					label = distributions[j] + "-" + druidversion[k]
					plt.plot(a, b, label=label)
	count += 1
	plt.legend()
	plt.show()

for i in xrange(len(brokermetrics)):
	if not brokermetrics:
		continue
	fig = plt.figure(count)
	fig.suptitle(brokermetrics[i])
	for x in xrange(int(numberofbroker[0])):
		for j in xrange(len(distributions)):
			for k in xrange(len(druidversion)):
				filename = pathtoresults[0] + "/result-" + distributions[j] + "-" + druidversion[k] + "/broker-" + str(x) + "-" + brokermetrics[i] + ".log"
				a = []
				b = []
				with open(filename) as csvfile:
					plots = csv.reader(csvfile, delimiter='	')
					plots.next()
					count = 1
					for row in plots:
						a.append(count)
						b.append(float(row[1]))
						count += 1
				label = distributions[j] + "-" + druidversion[k]
				plt.plot(a, b, label=label)
	count += 1
	plt.legend()
	plt.show()


for i in xrange(len(coordinatormetrics)):
	if not coordinatormetrics:
		continue
	fig = plt.figure(count)
	fig.suptitle(coordinatormetrics[i])
	for x in xrange(int(numberofcoordinator[0])):
		for j in xrange(len(distributions)):
			for k in xrange(len(druidversion)):
				filename = pathtoresults[0] + "/result-" + distributions[j] + "-" + druidversion[k] + "/coordinator-" + str(x) + "-" + coordinatormetrics[i] + ".log"
				a = []
				b = []
				with open(filename) as csvfile:
					plots = csv.reader(csvfile, delimiter='	')
					plots.next()
					count = 1
					for row in plots:
						a.append(count)
						b.append(float(row[1]))
						count += 1
				label = distributions[j] + "-" + druidversion[k]
				plt.plot(a, b, label=label)
	count += 1
	plt.legend()
	plt.show()

if(throughputgraph[0] == "True" or throughputgraph[0] == "true"):
	fig = plt.figure(count)
	fig.suptitle('Throughput')
	a = dict()
	for j in xrange(len(distributions)):
		for k in xrange(len(druidversion)):
			filename = pathtoresults[0] + "/result-" + distributions[j] + "-" + druidversion[k] + "/workloadgenerator.log"
			with open(filename) as csvfile:
				plots = csv.reader(csvfile, delimiter=' ')
				for row in plots:
					if(row[1] == "Throughput:"):
						a.update({distributions[j] + "-" + druidversion[k]:row[2]})
	fig, ax1 = plt.subplots()
	testnames = a.keys()
	pos = np.arange(len(testnames))
	rects = ax1.barh(pos, [a[k] for k in testnames], align='center', tick_label=testNames)
	plt.legend()
	plt.show()
