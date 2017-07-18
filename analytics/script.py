import numpy as np
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD

# column numbers for the variables which may effect violent crime
input_columns = [1,2,3,4,5,6]
# column number for violent crime
output_column = 20

# parameters used in regreesion
NUM_ITERATIONS = 1000
STEP_SIZE = 0.001
INITIAL_WEIGHTS = None

# loads data from file
# then splits each line on \t and convert all numeric fields to float
# then transforms each row of data as LabeledPoint so that it could fed into learning algorithm
data = sc.textFile("hdfs:///user/ssg441/fine-crime-data") \
         .map(lambda line: map(lambda x: float(x) if x.replace('.', '', 1).isdigit() else x, line.split('\t'))) \
         .map(lambda row: LabeledPoint(row[output_column], [row[i] for i in input_columns]))

# divide data into test and training set
(trainingData, testData) = data.randomSplit([0.7, 0.3])

model = LinearRegressionWithSGD.train(trainingData, iterations=NUM_ITERATIONS, \
    step=STEP_SIZE, initialWeights=INITIAL_WEIGHTS)

# TODO
# Test model on testData

# print model
model

# save model in hdfs
model.save(sc, "myModel")
