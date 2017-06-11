#!/usr/bin/python

# Script plots the workload generator query inter departure times

import glob
import numpy as np
import matplotlib.pyplot as plt
import json
import _strptime
from datetime import datetime, timedelta
import re

def plotWGQueryTimes():
	# concatenate all workload files
	wgFiles = glob.glob("workloadgenerator-node-*.log")
	data = []
	for fname in wgFiles:
		with open(fname) as f:
			for line in f:
				l = line.rstrip('\n')
				if "Query id" in l:
					if ",time" not in l:
						date = l.split(" ")[0]+" "+l.split(" ")[1]
						time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
						data.append(time)
	data.sort()
	x = []
	y = []
	time = 0
	count = 0
	prevtime = datetime.now()
	cumulativetime = 0;
	for t in data:
		if count == 0:
			time = 0
		else:
			time = (t-prevtime).total_seconds()*1000
		cumulativetime = cumulativetime + time
		x.append(cumulativetime/1000)
		y.append(time)
		prevtime = t
		count = count + 1

	plt.plot(x, y, label='inter-departure time')
	plt.legend(loc='upper right', fontsize = 'small')
	plt.title('Workload generator query inter-departure times')
	plt.ylabel('Inter-departure time (ms)')
	plt.xlabel('Time')
	plt.savefig('workloadgenerator_plot.png')
	print "Median is "+str(np.median(y))
	print "Cumulative time is "+str(cumulativetime)

def main():
	plotWGQueryTimes()

main()

