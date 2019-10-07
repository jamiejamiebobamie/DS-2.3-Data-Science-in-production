from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD as lrSGD

spark = SparkSession \
    .builder \
    .appName("Python Spark regression example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv('./datasets/titanic.csv',header=True, inferSchema = True)

df.show(10)

print((df.count(), len(df.columns)))


"""
VARIABLE DESCRIPTIONS:
survival        Survival
                (0 = No; 1 = Yes)
pclass          Passenger Class
                (1 = 1st; 2 = 2nd; 3 = 3rd)
name            Name
sex             Sex
age             Age
sibsp           Number of Siblings/Spouses Aboard
parch           Number of Parents/Children Aboard
ticket          Ticket Number
fare            Passenger Fare
cabin           Cabin
embarked        Port of Embarkation
                (C = Cherbourg; Q = Queenstown; S = Southampton)
"""

# how many of Age values are null
from pyspark.sql.functions import isnan, when, count, col
df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()


# df = df.drop('_c0')
#
# regressionDataFrame.show(10)
#
# regressionDataRDD = regressionDataFrame.rdd.map(list)
#
#
# regressionDataLabelPoint = regressionDataRDD.map(lambda data : LabeledPoint(data[3], data[0:3]))
#
# regressionLabelPointSplit = regressionDataLabelPoint.randomSplit([0.7, 0.3])
#
# regressionLabelPointTrainData = regressionLabelPointSplit[0]
#
# regressionLabelPointTestData = regressionLabelPointSplit[1]
#
#
# ourModelWithLinearRegression  = lrSGD.train(data = regressionLabelPointTrainData,
#                                             iterations = 200, step = 0.02, intercept = True)
