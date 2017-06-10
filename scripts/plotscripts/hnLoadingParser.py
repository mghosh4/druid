#!/usr/bin/python

import glob

def parseBrokerFiles():
	
	outputFile = open('broker_min_load.log', 'w')
	brokerFiles = glob.glob("broker-*.log")
	for fname in brokerFiles:
		with open(fname) as f:
			for line in f:
				l = line.rstrip('\n')
				if 'Choosing' in l:
					values = [p.split(']')[0] for p in l.split('[') if ']' in p]
					outputFile.write(l.split(",")[0]+","+values[2]+","+values[3]+"\n")

	outputFile.close()

def main():
	parseBrokerFiles()

main()

