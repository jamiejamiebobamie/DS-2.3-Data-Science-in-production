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


histogram_of_genders = {'male':0,'female':0}

for row in df.collect():
    if row.Sex:
        histogram_of_genders[row.Sex]+=1

print(histogram_of_genders)

average_age_of_men = 0
average_age_of_women = 0

men_count = 0
women_count = 0

for row in df_only_C.collect():
    if row.Sex == 'female':
        if row.Age:
            women_count+=1
            average_age_of_women += row.Age
    else:
        if row.Sex == 'male':
            if row.Age:
                men_count+=1
                average_age_of_men += row.Age

average_age_of_men /= men_count
average_age_of_women /= women_count


print("men: "+str(average_age_of_men)+" women: "+str(average_age_of_women))


woman_ages = []
man_ages = []
max_man_age = float('-inf')
max_woman_age = float('-inf')
for row in df_only_C.collect():
    if row.Sex == 'female':
        if row.Age:
            if row.Age > max_woman_age:
                max_woman_age = row.Age
            # max_woman_age = max(max_woman_age, row.Age)
    else:
        if row.Sex == 'male':
            if row.Age:
                if row.Age > max_man_age:
                    max_man_age = row.Age
                # man_ages.append(row.Age)
                # max_man_age = max(max_man_age, row.Age)


print("men: "+str(max_man_age)+" women: "+str(max_woman_age))
