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

shape = (df.count(), len(df.columns))
print(shape)


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
from pyspark.sql.functions import *
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# create a new column as gender, when Sex is female it is zero when sex is male it is one
df\
.withColumn('Gender',when(df.Sex == 'male',1).otherwise(0))\
.show(10)

shape = (df.count(), len(df.columns))
print(shape)

# List all of the Ages that are not null
df_not_null_ages = df.filter(df.Age.isNotNull())
df_not_null_ages.show(10)


# Slice the dataframe for those whose Embarked section was 'C'
df_only_C = df.filter(df.Embarked == 'C')
df_only_C.show(10)

# Describe a specific column
df.describe().show()

df.select('Embarked').distinct().show()
df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns)).show()
