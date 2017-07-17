# Instructions

## LogIn to Dumbo

## Load python 2.7, default is 2.6 and numpy doesnt work in that
`module load python/gnu/2.7.11`

## start pyspark
`pyspark`

## To exit pyspark
`quit()`

## import MlLib classes
```
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
```

## Example with Dummy data
LabeledPoint is a container for each data point while passing it as parameter to learning algorithms. The data will be an Array/List of LabeledPoints.  
eg: If we have one data point as `input <=> x` and `output <=> y`, to create LabeledPoint from this data point we can use the constructor as `LabeledPoint(y, x)` as demonstrated below:  
```
data = [
    LabeledPoint(0.0, [0.0, 1.0]),
    LabeledPoint(1.0, [1.0, 0.0]),
]
```

## Trains the model on given data
We can replace LogisticRegressionWithSGD here with any other model:
```
my_LRS = LogisticRegressionWithSGD.train(sc.parallelize(data), iterations=10)
```

## Commands to view the weights of various fields and to predict output of given a data point.
```
my_LRS
my_LRS.predict([1.0, 0.0])
```

## To load data from file we use SparkContext. SparkContext is provided as "sc" when we launch pySpark
```
file_data = sc.textFile("hdfs:///class1/temperatureInputs.txt")
```

## We can perform various operations while loading data from file
```
file_data = sc.textFile("hdfs:///class1/temperatureInputs.txt")
                .map(lambda line: line.split(','))
                .filter(lambda line: len(line) >= 10)
                .collect()
```
