#!/usr/bin/python

# Script plots the access patterns of segments across HNs

import glob
import numpy as np
import matplotlib.pyplot as plt
import json
import _strptime
from datetime import datetime, timedelta
import re
import math

def plotHnSegmentAccess():
    #matplotlib.colors.cnames
    colors = ['b', 'g', 'r', 'k', 'm', 'y', 'c']
    segmentYaxis = [0.5, 1, 1.5, 2, 2.5, 3, 3.5]
    
    # read the historical segment scan files to plot query boundaries
    hnames = glob.glob("historical-*-segment-scan-pending.log")
    if len(hnames) == 0:
        print "Error: historical-*-segment-scan-pending.log files missing"
    minQueryStartTime = datetime.now() + timedelta(1000) # add 1000 days
    maxQueryEndTime = datetime.now() - timedelta(1000) # subtrace 1000 days
    for hname in hnames:
        with open(hname) as f:
            for line in f:
                l = line.rstrip('\n').replace("\t", " ")
                lsplit = l.split(" ")
                date = lsplit[0]+" "+lsplit[1]
                if len(lsplit[1].split(".")) == 1:
                	time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                else:	
                	time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
                if time < minQueryStartTime:
                    minQueryStartTime = time
                if time > maxQueryEndTime:
                    maxQueryEndTime = time
    
    totalQueryDurationInSecs = int(math.ceil(float((maxQueryEndTime - minQueryStartTime).total_seconds())/60))
    segGenInterval = 60 # assuming segment generation interval is every 1 minute

    fnames = glob.glob("coordinator-0.log")
    if len(fnames) == 0:
        print "Error: coordinator-0.log file not found"
        return
    fname = fnames[0]
    firsttime = ''
    data = {}
    metricslist = []
    lasttime = datetime.now()
    hnmetricplots = {}
    segtimeplots = {}
    with open(fname) as f:
        for line in f:
            l = line.rstrip('\n')
            lsplit = l.split(" ")
            date = lsplit[0]+" "+lsplit[1]    
            time = datetime.strptime(date, '%Y-%m-%d %H:%M:%S,%f')

            if "Insert Segment" in line and " to " in line:
                if firsttime == '':
                    firsttime = time
                metric = lsplit[-3][1:-1] 
                hn = lsplit[-1][1:-1].split(":")[0]

                if metric not in metricslist:
                    metricslist.append(metric)

                if hn in hnmetricplots:
                    metricplots = hnmetricplots[hn]
                    if metric in metricplots:
                        print "Error 1!!"
                    else:
                        metricplots[metric] = time
                else:
                    metricplots = {}
                    metricplots[metric] = time
                    hnmetricplots[hn] = metricplots

                #print "inserted in "+str(hn)+" metric "+str(metricslist.index(metric))+" at time "+date

            if "Remove Segment" in line and " from " in line:
                metric = lsplit[-3][1:-1]
                hn = lsplit[-1][1:-1].split(":")[0]
                if hn in hnmetricplots:
                    metricplots = hnmetricplots[hn]
                    if metric in metricplots:
                        starttime = metricplots.pop(metric)
                        if hn in segtimeplots:
                            segplot = segtimeplots[hn]

                            # find the list with matching metric_id
                            itemadded = False
                            for item in segplot:
                                if item[0] == metricslist.index(metric):
                                    item[1].append([[(starttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]])
                                    itemadded = True
                            if itemadded == False:
                                segplot.append([metricslist.index(metric), [[[(starttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]]]])
                            #print "a removed from "+str(hn)+" metric "+str(metric)+" at time "+date
                        else:
                            segplot = []
                            segplot.append([metricslist.index(metric), [[[(starttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]]]])
                            segtimeplots[hn] = segplot
                            #print "b removed from "+str(hn)+" metric "+str(metricslist.index(metric))+" at time "+date
                    else:
                        print "Error 2!!"
                else:
                    print "Error 3!!"
                        
            if "Segment Received" in l and "from" in l:
                metric = lsplit[7][1:-1]
                numscans = lsplit[10][1:-1]
                hn = lsplit[12][1:-1]
                if hn in data:
                    templist = data[hn]
                    templist.append([time, metric, numscans])
                else:
                    templist = []
                    templist.append([time, metric, numscans])
                    data[hn] = templist
                if metric not in metricslist:
                    metricslist.append(metric)
                
                lasttime = time

    # add additional 30secs to the last time
    lasttime = lasttime + timedelta(minutes=1)

    # sort the metric list
    metricslist.sort()

    # loop thorugh the remaining hnmetricplots and set their endtimes
    for hn, metrics in hnmetricplots.iteritems():
        for metric, time in metrics.iteritems():
            if hn in segtimeplots:
                segplot = segtimeplots[hn]
                # find the list with matching metric_id
                itemadded = False
                for item in segplot:
                    if item[0] == metricslist.index(metric):
                        item[1].append([[(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(lasttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]])
                        itemadded = True
                if itemadded == False:
                    segplot.append([metricslist.index(metric), [[[(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(lasttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]]]])
            else:
                segplot = []
                segplot.append([metricslist.index(metric), [[[(time-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]], [(lasttime-firsttime).total_seconds(), segmentYaxis[metricslist.index(metric)]]]]])
                segtimeplots[hn] = segplot
                #print "b removed from "+str(hn)+" metric "+str(metricslist.index(metric))+" at time "+str(lasttime)

    # process one HN at a time
    count = 0
    for hn, datalist in data.iteritems():
        x = []
        y = []
        time = 0
        firstentry = True
        prevtime = firsttime
        cumulativetime = 0;
        for entry in datalist:
            time = (entry[0]-prevtime).total_seconds()*1000
            cumulativetime = cumulativetime + time
            prevtime = entry[0]
            x.append(cumulativetime/1000)
            y.append(np.log(int(entry[2])))
            plt.text(x[-1], y[-1], metricslist.index(entry[1]), fontsize=12, horizontalalignment='left', verticalalignment='bottom')

        # plot the segment accesses
        plt.plot(x, y, 'k.', markersize=10, label='scan time')
        # plot the individual segment durations
        segplot = segtimeplots[hn]
        segplotsorted = sorted(segplot,key=lambda x: x[1])
        for item in segplotsorted:
            templist = item[1]
            newarray = np.array(templist)
            xtemp, ytemp = newarray.T
            plt.plot(xtemp, ytemp, ''+colors[int(item[0])]+'-', label='segment-'+str(item[0]), linewidth = '10')

        # plot the segment generation intervals
        yseg = [0, max(y)+2]
        for i in range(0,(totalQueryDurationInSecs+1)):
            xseg = list([(minQueryStartTime+timedelta(minutes=int(i))-firsttime).total_seconds(), (minQueryStartTime+timedelta(minutes=int(i))-firsttime).total_seconds()])
            plt.plot(xseg, yseg, 'r-')

        plt.legend(loc='upper right', fontsize = 'xx-small')
        plt.title('HN '+str(hn.split(".")[0])+' segment access across time')
        plt.ylabel('Total segment access time milliseconds (log-e values)', fontsize=10)
        plt.xlabel('Time (secs)', fontsize=10)
        plt.xticks(np.arange(0, max(x)+90.0, 30), fontsize=9) # arrange ticks on 30secs boundary
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

