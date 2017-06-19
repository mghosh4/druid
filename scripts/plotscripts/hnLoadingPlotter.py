#!/usr/bin/python

# Script plots the historical node loading (HN queue size + HN active threads)
# for all HNs. It also plots the broker query start time at y_axis = 5 units.
# Script takes numHNPerPlot as an optional argument (default is 15)

import glob
import numpy as np
import matplotlib.pyplot as plt
import json
import _strptime
from datetime import datetime, timedelta
import re
import sys

# resolution in milliseconds, value 250 aggregates for 250ms
resolution = 1000

def getBrokerActivityData():
    global resolution
                    
    # concatenate all broker files
    brokerFiles = glob.glob("broker-*-query-time.log")
    data = []
    y = []
    for fname in brokerFiles:
        with open(fname) as f:
            firstline = True
            for line in f:
                # skip the first query time since it is for a dummy query
                if firstline == True:
                    firstline = False
                    continue

                l = line.rstrip('\n').split('\t')
                time = datetime.now()
                if len(l[0].split(".")) != 2:
                    time = datetime.strptime(l[0], '%Y-%m-%d %H:%M:%S')
                else:
                    time = datetime.strptime(l[0], '%Y-%m-%d %H:%M:%S.%f')
                time = time - timedelta(milliseconds=int(float(l[1])))
                data.append(time)
                y.append(5)

    data.sort()
    x = []
    time = 0
    prevtime = data[0]
    for point in data:
        currtime = point
        time = time + (currtime - prevtime).total_seconds()*1000/resolution
        x.append(time)
        prevtime = currtime

    return x, y


numbers = re.compile(r'(\d+)')
def numericalSort(value):
    parts = numbers.split(value)
    parts[1::2] = map(int, parts[1::2])
    return parts

def plotHNLoading():
    global resolution
    bx, by = getBrokerActivityData()
    hnMap = []
    hnMapFiles = glob.glob("historical-to-hostname-map.log")
    if len(hnMapFiles) == 0:
        print "Error: historical-to-hostname-map.log file missing"
        return
    with open(hnMapFiles[0]) as f:
        for line in f:
            hnMap.append(line.rstrip('\n').split("\t")[1].split(".")[0])

    hnFiles = sorted(glob.glob("historical-*-segment-scan-pending.log"), key=numericalSort)
    if len(hnFiles) == 0:
        print "Error: historical-*-segment-scan-pending.log files missing"
        return
    loop = 0
    numHNPerPlot = 15
    if len(sys.argv) == 2:
        numHNPerPlot = int(sys.argv[1])
    else:
        print "INFO: Script is setup to plot "+str(numHNPerPlot)+" lines per plot by default"
    maxPlotValue = 0
    for fname in hnFiles:
        with open(fname) as f:
            x = []
            y = []
            samples = 0
            prevtime = datetime.now()
            loadingSum = 0
            count = 0
            firstsample = True
            time = 0
            for line in f:
                l = line.rstrip('\n').split('\t')
                currtime = datetime.now()

                if len(l[0].split(".")) != 2:
                    currtime = datetime.strptime(l[0], '%Y-%m-%d %H:%M:%S')                    
                else:
                    currtime = datetime.strptime(l[0], '%Y-%m-%d %H:%M:%S.%f')
                
                if firstsample == True :
                    x.append(0)
                    y.append(float(l[1]))
                    firstsample = False
                    prevtime = currtime
                else:    
                    if(currtime-prevtime).total_seconds()*1000 <= resolution:
                        loadingSum = loadingSum + float(l[1])
                        count = count + 1
                    else:
                        time = time + (currtime - prevtime).total_seconds()*1000/resolution
                        #time = time + 1
                        x.append(time)
                        if count == 0:
                            y.append(float(l[1]))
                            maxPlotValue = max(maxPlotValue, float(l[1]))
                        else:
                            y.append(float(loadingSum)/float(count))
                            maxPlotValue = max(maxPlotValue, float(loadingSum)/float(count))
                            count  = 1
                            loadingSum = float(l[1])
                        prevtime = currtime

            plt.plot(x, y, label='hn_'+hnMap[int(fname.split("-")[1])]+'_loading')
            loop = loop + 1
            if loop % numHNPerPlot == 0:
                plt.plot(bx, by, '.', label='broker query activity')
                if (numHNPerPlot > 5) :
                    plt.legend(loc='upper left', fontsize = 'xx-small', ncol=4)
                else:
                    plt.legend(loc='upper left', fontsize = 'xx-small')
                plt.ylim(0, 1.25*int(maxPlotValue))
                if numHNPerPlot > 1:
                    plt.title('HN '+str(loop-numHNPerPlot)+' to '+str(loop-1)+' Loading')
                else:
                    plt.title('HN-'+str(loop)+" "+hnMap[int(fname.split("-")[1])]+' Loading')
                plt.ylabel('Loading (HN queue size + active threads)')
                plt.xlabel('Time (each tick is '+str(float(resolution)/1000)+' secs)')
                plt.xticks(np.arange(0, max(bx)+90.0, 30), fontsize=10) # arrange ticks on 30secs boundary
                plt.grid(True)
                outputPlotFile = ''
                if numHNPerPlot > 1:
                    outputPlotFile = 'hn_'+str(loop-numHNPerPlot)+'_to_'+str(loop-1)+'_loading.png'
                else:
                    outputPlotFile = 'hn_'+str(loop)+"_"+hnMap[int(fname.split("-")[1])]+'_loading.png'
                plt.savefig(outputPlotFile)
                plt.clf()
                maxPlotValue = 0
                print "Output plot : "+outputPlotFile

def main():
    plotHNLoading()

main()

