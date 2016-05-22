from __future__ import print_function
import urllib2
import string
import urllib
import sched, time
import json
from datetime import datetime, timedelta, date

def segmentcount(log):
	intervalarray = json.loads(urllib2.urlopen("http://node-1.druidthomas.dcsq.emulab.net:8080/druid/coordinator/v1/datasources/metrics/intervals").read())
	totalsegments = 0
	for interval in intervalarray:

		interval2 = string.replace(interval, "/", "%2F")
		url = "http://node-1.druidthomas.dcsq.emulab.net:8080/druid/coordinator/v1/datasources/metrics/intervals/" + interval2 + "/serverview"
		try:
			serverlist = json.loads(urllib2.urlopen(url).read())[0]["servers"]
			totalsegments += len(serverlist)
			printstatement = "Segment Interval: " + interval + ", Replicas: " + `len(serverlist)`
			print(printstatement, file = log)
		except IndexError:
			continue
	printstring = "Time: " + datetime.now().strftime('%H:%M:%S') + ", Total Segments: " + `totalsegments`
	print(printstring, file = log)

def memoryutilization(log):
	historical1 = json.loads(urllib2.urlopen("http://node-2.druidthomas.dcsq.emulab.net:8081/status").read())
	historical2 = json.loads(urllib2.urlopen("http://node-3.druidthomas.dcsq.emulab.net:8081/status").read())
	historical3 = json.loads(urllib2.urlopen("http://node-4.druidthomas.dcsq.emulab.net:8081/status").read())

	usedmemory = float(historical1['memory']['usedMemory']) + float(historical2['memory']['usedMemory']) + float(historical3['memory']['usedMemory'])
	totalmemory = float(historical1['memory']['totalMemory']) + float(historical2['memory']['totalMemory']) + float(historical3['memory']['totalMemory'])
	memoryutilization = float(usedmemory)/float(totalmemory)

	printstring = "Time: " + datetime.now().strftime('%H:%M:%S')  + ", Used Memory: " + `usedmemory`
	print(printstring, file = log)


log1 = open("memoryusage.log", "a")
endtime = datetime.now() + timedelta(minutes=15)
while True:
		if datetime.now() >= endtime:
			break

		memoryutilization(log1)
		segmentcount(log1)
