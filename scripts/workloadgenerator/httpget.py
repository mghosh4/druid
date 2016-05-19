from __future__ import print_function
import urllib2
import sched, time
import json
from datetime import datetime, timedelta

endtime = datetime.now() + timedelta(minutes=10)
while True:
		if datetime.now() >= endtime:
			break
		historical1 = json.loads(urllib2.urlopen("http://node-2.druidthomas.dcsq.emulab.net:8081/status").read())
		historical2 = json.loads(urllib2.urlopen("http://node-3.druidthomas.dcsq.emulab.net:8081/status").read())
		historical3 = json.loads(urllib2.urlopen("http://node-3.druidthomas.dcsq.emulab.net:8081/status").read())
	
		utilization1 = float(historical1['memory']['usedMemory'])/float(historical1['memory']['totalMemory'])
		utilization2 = float(historical2['memory']['usedMemory'])/float(historical2['memory']['totalMemory'])
		utilization3 = float(historical3['memory']['usedMemory'])/float(historical3['memory']['totalMemory'])

		log1 = open("historical1usage.txt", "a")
		log2 = open("historical2usage.txt", "a")
		log3 = open("historical3usage.txt", "a")

		print(utilization1, file = log1)
		print(utilization2, file = log2)
		print(utilization3, file = log3)