# This script was used to predict the response time for complaints
# using the random forest regression algorithm. The features
# chosen with “Agency”, “Agency Name”, “Complaint Type”, “Descriptor”, 
# “Incident Zip”, “Borough”, “X-coordinate” and “Y-coordinate”. 

from pyspark import SparkContext
from pyspark.sql import SQLContext
import operator
import datetime
import calendar
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

# Converts a string data to datetime object
def strToTS(sDate):    
	pDt = datetime.datetime.strptime(sDate, "%m/%d/%Y %I:%M:%S %p")
	return pDt

# This function filters the data and then returns all the features
# for each filtered record
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
						yield (row[3], row[4], row[5], row[6], row[8], row[24], row[25], row[26], (row[2] - row[1]).total_seconds())

			elif row[8] in BrookZipList:
				row[24] = 'BROOKLYN'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], row[4], row[5], row[6], row[8], row[24], row[25], row[26], (row[2] - row[1]).total_seconds())

			elif row[8] in QueenZipList:
				row[24] = 'QUEENS'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], row[4], row[5], row[6], row[8], row[24], row[25], row[26], (row[2] - row[1]).total_seconds())

			elif row[8] in ManZipList:
				row[24] = 'MANHATTAN'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], row[4], row[5], row[6], row[8], row[24], row[25], row[26], (row[2] - row[1]).total_seconds())

			elif row[8] in StatenZipList:
				row[24] = 'STATEN ISLAND'
				if row[1] and row[2]:
					row[1] = strToTS(row[1])
					row[2] = strToTS(row[2])
					if (row[2] - row[1]).total_seconds() > 0 and (row[2].year and row[1].year in xrange(2010, 2017)):
						yield (row[3], row[4], row[5], row[6], row[8], row[24], row[25], row[26], (row[2] - row[1]).total_seconds())


if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	# Read the 311.csv file into rdd
	rdd = sc.textFile('311.csv', use_unicode=False)

    # Apply the filter to rdd
	rdd = rdd.mapPartitionsWithIndex(respTime)

	# Convert rdd to dataframe and give column names
	df = rdd.toDF(["Agency","AgencyName","ComplaintType","Descriptor","IncidentZip","Borough","XCoord","YCoord","ResTime"])

	# Drop all records with any missing field
	df = df.dropna(how='any')

	# Convert all categorical features to dummy variables
	indexerAgency = StringIndexer(inputCol="Agency", outputCol="agencyIndex", handleInvalid="skip")
	indexerAName = StringIndexer(inputCol="AgencyName", outputCol="ANameIndex", handleInvalid="skip")
	indexerComType = StringIndexer(inputCol="ComplaintType", outputCol="CTypeIndex", handleInvalid="skip")
	indexerDesc = StringIndexer(inputCol="Descriptor", outputCol="DescIndex", handleInvalid="skip")
	indexerIZip = StringIndexer(inputCol="IncidentZip", outputCol="IZipIndex", handleInvalid="skip")
	indexerBoro = StringIndexer(inputCol="Borough", outputCol="bIndex", handleInvalid="skip")

	# Convert Xcoord and Ycoord features to double type
	df = df.withColumn("XCoord",df["XCoord"].cast("double")).withColumn("YCoord",df["YCoord"].cast("double"))

	# Transform the original df by running stringIndexers to convert catrgorical variable to dummy 
	pipelineTmp = Pipeline(stages=[indexerAgency,indexerAName,indexerComType,indexerDesc,indexerIZip,indexerBoro])
	model = pipelineTmp.fit(df)
    newDf = model.transform(df)

    # Convert the resulting dataframe to rdd
	rdd = newDf.rdd

	# Convert each point to a labled point type
	data = rdd.map(lambda x: LabeledPoint(x[8], x[6:8]+x[9:]))

	# Split the data into training and testing
	(trainingData, testData) = data.randomSplit([0.7, 0.3])

	# train the randomforest regressor using training data, the numTrees was set to 3,7 and 16
	model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo={2:20, 3:1664, 4:257, 5:1145, 6:220, 7:5}, numTrees=3, featureSubsetStrategy="auto", impurity='variance', maxDepth=10, maxBins=1664)

	# predict response time using the model
	predictions = model.predict(testData.map(lambda x: x.features))

	#Zip the predicted and original labels
	labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

	# Compute the MSE(mean square error)
	testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum()/float(testData.count())

	# Covert the result to rdd
	resultRdd = sc.parallelize([testMSE])

	# Save the result
	resultRdd.coalesce(1,True).saveAsTextFile("311ResMse")