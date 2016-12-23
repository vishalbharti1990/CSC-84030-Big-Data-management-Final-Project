# This file is used to filter the rdd by incident zip and 
# then a filtered dataset is created using
# ("Agency", "Response Time") as key
# Response time = Closed Date - Created Date 

from pyspark import SparkContext
from pyspark.sql import SQLContext
import operator
import datetime
import calendar

# Converts a string data to datetime object
def strToTS(sDate):    
	pDt = datetime.datetime.strptime(sDate, "%m/%d/%Y %I:%M:%S %p")
	return pDt

# The mapper filters the dataset based on the zip : If zip is missing the record is dropped
# the borough is then assigned using Incident Zip, then return (key, 1) for each record
def respTime(index, lines):
	import csv
	import calendar
	if index==0:
		lines.next()
	reader = csv.reader(lines)

	BronxZipList = [str(x) for x in range(10451,10476)]
        BrookZipList = ['11201']+[str(x) for x in range(11202,11240)]+['11241','11242','11243','11249','11252','11256']
        QueenZipList = ['11351']+[str(x) for x in range(11354,11376)]+['11377','11378','11379','11385']+[str(x) for x in range(11411,11424)]+['11426','11427','11428','11429','11430','11432','11433','11434','11435','11436','11691','11692','11693','11694','11697']
        ManZipList = [str(x) for x in range(10001,10008)]+[str(x) for x in range(10009,10042)]+['10044','10045','10048','10055','10060','10069','10090','10095','10098','10099','10103','10104','10105','10106','10107','10110','10111','10112','10115','10118','10119','10120','10121','10122','10123','10128','10151','10152','10153','10154','10155','10158','10161','10162']+[str(x) for x in range(10165,10179)]+['10199','10270','10271']+[str(x) for x in range(10278,10283)]
        StatenZipList = [str(x) for x in range(10301,10315)]

	for row in reader:
		if row[8]:
			if row[8] in BronxZipList:
				row[24] = 'BRONX'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], (row[2] - row[1]).total_seconds())

			elif row[8] in BrookZipList:
				row[24] = 'BROOKLYN'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], (row[2] - row[1]).total_seconds())

			elif row[8] in QueenZipList:
				row[24] = 'QUEENS'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], (row[2] - row[1]).total_seconds())

			elif row[8] in ManZipList:
				row[24] = 'MANHATTAN'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], (row[2] - row[1]).total_seconds())

			elif row[8] in StatenZipList:
				row[24] = 'STATEN ISLAND'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], (row[2] - row[1]).total_seconds())


if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	rdd = sc.textFile('311.csv', use_unicode=False)

    # Filter the data and calculate average response time for each agency
	rdd = rdd.mapPartitionsWithIndex(respTime).mapValues(lambda v: (v,1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda v: (v[0]/float(v[1]), v[1]) )

	# Save the resulting rdd as text file
	rdd.coalesce(1,True).saveAsTextFile("311ResTime")
