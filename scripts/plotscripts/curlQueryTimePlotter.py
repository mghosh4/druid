#!/usr/bin/python

# Script plots the curl query inter departure times and curl query times

import glob
import numpy as np
import matplotlib.pyplot as plt
import json
import _strptime
from datetime import datetime, timedelta
import re

def plotCurlQueryTimes():
	# concatenate all workload files
	wgFiles = glob.glob("workloadgenerator-node-19.log")
	querysendtime = []
	resulttime = []
	connecttime = []
	querytotaltime = []
	querygentocurlsenddelay = []
	for fname in wgFiles:
		firstsample = True
		linenum = 0
		with open(fname) as f:
			lines = f.read().splitlines()
			for line in lines:
				if "> POST" in line:
					l = line.rstrip('\n').split(" ")
					date = l[0]+" "+l[1]
					time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
					querysendtime.append(time)
				if "200 OK" in line:
					l = line.rstrip('\n').split(" ")
					date = l[0]+" "+l[1]
					time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
					resulttime.append(time)
				if "Query id" in line and ",time" not in line:
					duration = "\""+line.split(" ")[-1]+"\", \""
					date = line.split(" ")[0]+" "+line.split(" ")[1]
					starttime = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
					loop = linenum + 1
					skipFile = False
					while True:
						templine = lines[loop]
						if "Error" in templine:
							skipFile = True
							break

						if duration in templine:
							date = templine.split(" ")[0]+" "+templine.split(" ")[1]
							endtime = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')
							querygentocurlsenddelay.append([starttime, ((endtime-starttime).total_seconds()*1000)])
							break
						loop = loop + 1
					if skipFile == True:
						break

				#if "Connect_time" in line:
				linenum = linenum + 1

	# plot query inter departure times
	maxqueryinterdeparturetime = 0
	querysendtime.sort()
	x = []
	y = []
	time = 0
	firstsample = True
	prevtime = datetime.now()
	cumulativetime = 0;
	for t in querysendtime:
		if firstsample == True:
			time = 0
			firstsample = False
		else:
			time = (t-prevtime).total_seconds()*1000
		cumulativetime = cumulativetime + time
		x.append(cumulativetime/1000)
		y.append(time)
		if time > maxqueryinterdeparturetime:
			maxqueryinterdeparturetime = time
			maxtime = t
		prevtime = t

	plt.plot(x, y, label='inter-departure time')
	plt.legend(loc='upper right', fontsize = 'small')
	plt.title('Curl query inter-departure time')
	plt.ylabel('Inter-departure time (ms)')
	plt.xlabel('Time (secs)')
	plt.ylim(0, max(y))
	plt.savefig('curl_query_interdeparture_plot.png')
	print "Output plot : curl_query_interdeparture_plot.png"
	plt.clf()
	print "Median is "+str(np.median(y))
	print "Max query inter-departure time is "+str(maxqueryinterdeparturetime)+" at time "+str(maxtime)+" first sample was at time "+str(querysendtime[0])

	# plot query result inter arrival times
	maxqueryresultinterarrivatime = 0
	resulttime.sort()
	x = []
	y = []
	time = 0
	firstsample = True
	prevtime = datetime.now()
	cumulativetime = 0;
	for t in resulttime:
		if firstsample == True:
			time = 0
			firstsample = False
		else:
			time = (t-prevtime).total_seconds()*1000
		cumulativetime = cumulativetime + time
		x.append(cumulativetime/1000)
		y.append(time)
		if time > maxqueryresultinterarrivatime:
			maxqueryresultinterarrivatime = time
			maxtime = t
		prevtime = t

	plt.plot(x, y, label='query result inter-arrival time')
	plt.legend(loc='upper right', fontsize = 'small')
	plt.title('Curl query result inter-arrival time')
	plt.ylabel('Inter-arrival time (ms)')
	plt.xlabel('Time (secs)')
	plt.ylim(0, max(y))
	plt.savefig('curl_query_result_interarrival_plot.png')
	print "Output plot : curl_query_result_interarrival_plot.png"
	plt.clf()
	print "Median is "+str(np.median(y))
	print "Max query result inter-arrival time is "+str(maxqueryresultinterarrivatime)+" at time "+str(maxtime)+" first sample was at time "+str(querysendtime[0])

	# plot workloadgenerator to curl query send delays 
	maxquerydelay = 0
	b = sorted(querygentocurlsenddelay,key=lambda t: t[0])
	maxindex, maxquerydelay = max(querygentocurlsenddelay, key=lambda item: item[1])
	x = []
	y = []
	time = 0
	firstsample = True
	prevtime = datetime.now()
	cumulativetime = 0;
	count = 0
	for t in querygentocurlsenddelay:
		if(count < 100):
			print t[0], t[1]
			count = count + 1
		if firstsample == True:
			time = 0
			firstsample = False
		else:
			time = (t[0]-prevtime).total_seconds()*1000
		cumulativetime = cumulativetime + time
		x.append(cumulativetime/1000)
		y.append(t[1])
		prevtime = t[0]

	plt.plot(x, y, label='query time delay')
	plt.legend(loc='upper right', fontsize = 'small')
	plt.title('Workload Generator to Curl query send delay time')
	plt.ylabel('Delay time (ms)')
	plt.xlabel('Time (secs)')
	plt.ylim(0, max(y))
	plt.savefig('curl_query_delay_plot.png')
	print "Output plot : curl_query_delay_plot.png"
	plt.clf()
	print "Median is "+str(np.median(y))
	print "Max query inter-departure time is "+str(maxquerydelay)+" at time "+str(maxindex)+" first sample was at time "+str(querygentocurlsenddelay[0])

def main():
	plotCurlQueryTimes()

main()

