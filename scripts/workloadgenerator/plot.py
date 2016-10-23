#from Utils import Utils
import time
import datetime as dt
from datetime import *
import json
from pprint import pprint
from random import randint
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta

def diff_month(d1, d2):
    return (d1.year - d2.year)*12 + d1.month - d2.month


with open("meh2.json") as data_file:
    data = json.load(data_file)
numEntries = len(data)


dict1={}
for i in xrange(numEntries):
    period_interval_str = data[i]["event"]["interval"]
    period_pair = period_interval_str.split("/")
    period_start = datetime.strptime(period_pair[0], '%Y-%m-%dT%H:%M:%S.%fZ')
    period_end = datetime.strptime(period_pair[1], '%Y-%m-%dT%H:%M:%S.%fZ')
    print period_start.year
    period_s=datetime(period_start.year, period_start.month, 1, 0,0,0,0)
    period_e=datetime(period_end.year, period_end.month, 1, 0,0,0,0)
    months=diff_month(period_e, period_s)
   
    for j in xrange(months):
        ptr=period_s+relativedelta(months=+j)
        #print ptr
        if(ptr in dict1):
            dict1[ptr]=dict1[ptr]+1
        else:
            dict1[ptr]=1
#print dict1
#for w in sorted(dict1, key=dict1.get):
#    print w, dict1[w]
for k in sorted(dict1):
    print k, dict1[k]

dict2={}
for (k,v) in dict1.items():
       
        if v in dict2:
            dict2[v]=dict2[v]+1
        else:
            dict2[v]=1   
sofar=0
dict3={}
#print dict2
for k in sorted(dict2):
   
    sofar=sofar+dict2[k]
    dict3[k]=sofar
#print dict3
print len(dict3)
for k in sorted(dict3):
    dict3[k]=dict3[k]/float(sofar)

#print dict3
lists = sorted(dict3.items())
x, y=zip(*lists)
#print x
#print y
#plt.plot(range(len(dict3)), dict3.values())
#plt.xticks(range(len(dict3)), dict3.keys())
plt.plot(x,y)
plt.show()
