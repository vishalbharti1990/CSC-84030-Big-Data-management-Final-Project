# This file is used to filter the rdd by incident zip and 
# then a filtered dataset is created using
# ("Borough", "Agency", "Agency Name", "Created month", "Created Year", "Created Day", "Incident Zip") as key

from pyspark import SparkContext
from pyspark.sql import SQLContext
import operator
import datetime
import calendar

# Converts a string data to datetime object
def strToTS(sDate):    
	pDt = datetime.datetime.strptime(sDate, "%m/%d/%Y %H:%M:%S %p")
	return pDt

# The mapper filters the dataset based on the zip : If zip is missing the record is dropped
# the borough is then assigned using Incident Zip, then return (key, 1) for each record 
def mapper(index, lines):
	import csv
	if index==0:
		lines.next()
	reader = csv.reader(lines)

	# Zip list for all boroughs
	BronxZipList = [str(x) for x in range(10451,10476)]
    BrookZipList = ['11201']+[str(x) for x in range(11202,11240)]+['11241','11242','11243','11249','11252','11256']
    QueenZipList = ['11351']+[str(x) for x in range(11354,11376)]+['11377','11378','11379','11385']+[str(x) for x in range(11411,11424)]+['11426','11427','11428','11429','11430','11432','11433','11434','11435','11436','11691','11692','11693','11694','11697']
    ManZipList = [str(x) for x in range(10001,10008)]+[str(x) for x in range(10009,10042)]+['10044','10045','10048','10055','10060','10069','10090','10095','10098','10099','10103','10104','10105','10106','10107','10110','10111','10112','10115','10118','10119','10120','10121','10122','10123','10128','10151','10152','10153','10154','10155','10158','10161','10162']+[str(x) for x in range(10165,10179)]+['10199','10270','10271']+[str(x) for x in range(10278,10283)]
    StatenZipList = [str(x) for x in range(10301,10315)]

    # Filtering each record using the zip
	for row in reader:
		if row[8]:
			if row[8] in BronxZipList:
				row[24] = 'BRONX'
        	        	# row[24]-Borough, row[8]-Incident Zip
				yield ((row[24], row[4], row[5], strToTS(row[1]).month, strToTS(row[1]).year, calendar.day_name[strToTS(row[1]).weekday()], row[8]), 1)	
			elif row[8] in BrookZipList:
				row[24] = 'BROOKLYN'
        	        	# row[24]-Borough, row[8]-Incident Zip
				yield ((row[24], row[4], row[5], strToTS(row[1]).month, strToTS(row[1]).year, calendar.day_name[strToTS(row[1]).weekday()], row[8]), 1)	
			elif row[8] in QueenZipList:
				row[24] = 'QUEENS'	
        	        	# row[24]-Borough, row[8]-Incident Zip
				yield ((row[24], row[4], row[5], strToTS(row[1]).month, strToTS(row[1]).year, calendar.day_name[strToTS(row[1]).weekday()], row[8]), 1)	
			elif row[8] in ManZipList:
				row[24] = 'MANHATTAN'
        	        	# row[24]-Borough, row[8]-Incident Zip
				yield ((row[24], row[4], row[5], strToTS(row[1]).month, strToTS(row[1]).year, calendar.day_name[strToTS(row[1]).weekday()], row[8]), 1)	
			elif row[8] in StatenZipList:
				row[24] = 'STATEN ISLAND'
        	        	# row[24]-Borough, row[8]-Incident Zip
				yield ((row[24], row[4], row[5], strToTS(row[1]).month, strToTS(row[1]).year, calendar.day_name[strToTS(row[1]).weekday()], row[8]), 1)	


if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	bList = ['BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND']

	# Read the "311.csv" file as rdd
	rdd = sc.textFile('311.csv', use_unicode=False)

	# Apply the filter function to the Rdd
	rdd = rdd.mapPartitionsWithIndex(mapper).reduceByKey(operator.add)

	# Save the resulting rdd as text file
	rdd.coalesce(1,True).saveAsTextFile("311NNOut")