#!/usr/bin/python

# Script plots the access patterns of segments across HNs

import glob
import numpy as np
import matplotlib.pyplot as plt
import json
import _strptime
from datetime import datetime, timedelta
import re

def plotHnSegmentAccess():
	#matplotlib.colors.cnames
	fname = glob.glob("coordinator-0.log")[0]
	data = {}
	metricslist = []
	with open(fname) as f:
		for line in f:
			l = line.rstrip('\n')
			if "HN" in l:
				lsplit = l.split(" ")
				date = lsplit[0]+" "+lsplit[1]	
				time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
				hn = lsplit[6][1:-1]
				metric = lsplit[9][1:-1]
				numscans = lsplit[12][1:-1]
				if hn in data:
					templist = data[hn]
					templist.append([time, metric, numscans])
				else:
					templist = []
					templist.append([time, metric, numscans])
					data[hn] = templist
				if metric not in metricslist:
					metricslist.append(metric)

	# sort the metric list
	metricslist.sort()

	# process one HN at a time
	count = 0
	for hn, datalist in data.iteritems():
		x = []
		y = []
		time = 0
		firstentry = True
		prevtime = datetime.now()
		cumulativetime = 0;
		for entry in datalist:
			if firstentry == True:
				time = 0
				firstentry = False
			else:
				time = (entry[0]-prevtime).total_seconds()*1000
			cumulativetime = cumulativetime + time
			prevtime = entry[0]
			x.append(cumulativetime/1000)
			y.append(np.log(int(entry[2])))
			plt.text(x[-1], y[-1], metricslist.index(entry[1]), fontsize=12, horizontalalignment='left', verticalalignment='bottom')

		plt.plot(x, y, 'co--', markersize=10, label='hn num segment scans ')
		plt.legend(loc='upper left', fontsize = 'small')
		plt.title('HN '+str(count)+' segment access across time')
		plt.ylabel('Num segment scans (log scale)')
		plt.xlabel('Time (secs)')
		plt.ylim(0, float(1.25*max(y)))
		plt.grid(True)
		plt.savefig('hn_'+str(count)+'_segment_scan.png')
		plt.clf()
		count = count + 1

	print "Metric IDs"
	for i in range(0, len(metricslist)):
		print " "+str(i)+" : "+metricslist[i]

def main():
	plotHnSegmentAccess()

main()

