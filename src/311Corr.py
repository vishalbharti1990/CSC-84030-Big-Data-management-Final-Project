# This scipt was used to calculate correlation between the total complaints
# and response time.

from pyspark import SparkContext
from pyspark.sql import SQLContext
import numpy as np
from pyspark.mllib.stat import Statistics
import operator
import datetime
import calendar

# Converts a string data to datetime object
def strToTS(sDate):    
	pDt = datetime.datetime.strptime(sDate, "%m/%d/%Y %I:%M:%S %p")
	return pDt

def respTime(index, lines):
	import csv
	import calendar
	if index==0:
		lines.next()
	reader = csv.reader(lines)
	for row in reader:
		if row[1] and row[2]: 
			row[1] = strToTS(row[1])
			row[2] = strToTS(row[2])
			if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010,2017)):
				yield (row[4], (row[2] - row[1]).total_seconds())

if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	rdd = sc.textFile('311.csv', use_unicode=False)

    # Filter the data and calculate average response time for each agency
	rdd = rdd.mapPartitionsWithIndex(respTime).mapValues(lambda v: (v,1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).map(lambda v: (v[1][0]/float(v[1][1]), v[1][1]) )

	# Compute the pearson R value between "total calls" and "response time"
	corr = Statistics.corr(rdd, method="pearson")

	# Convert result to rdd
	rddC = sc.parallelize(corr)

	# Save the result
	rddC.coalesce(1,True).saveAsTextFile("311CorrOut")