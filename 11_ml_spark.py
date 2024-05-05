
import pandas as pd
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator





spark = pyspark.sql.SparkSession.builder.appName("ml").getOrCreate()
spark

df=spark.read.format("csv").option("header","true").load("data/train.csv")
df.show()

df.toPandas().head()
df.count()
df.dtypes
df.printSchema()
df.describe().toPandas()


data_schema=[
    StructField("PassengerId",IntegerType(),True),
    StructField("Survived",IntegerType(),True),
    StructField("Pclass",IntegerType(),True),
    StructField("Name",StringType(),True),
    StructField("Sex",StringType(),True),
    StructField("Age",FloatType(),True),
    StructField("SibSp",IntegerType(),True),
    StructField("Parch",IntegerType(),True),
    StructField("Ticket",StringType(),True),
    StructField("Fare",FloatType(),True),
    StructField("Cabin",StringType(),True),
    StructField("Embarked",StringType(),True)
]

final_struc=StructType(fields=data_schema)
final_struc 
df=spark.read.csv("data/train.csv",schema=final_struc,sep=',',header=True)
df.printSchema()
df.show()

dataset = df.select(df['Survived'].cast('float'),df['Pclass'].cast('float'),df['Sex'],df['Age'],df['Fare'],df['Embarked'])
dataset.show()

dataset.select([count(when(isnull(c),c)).alias(c) for c in dataset.columns]).show()
dataset=dataset.replace("null",None).dropna(how='any') #Remove rows with null values
dataset.show()

#Transform text values into numeric
dataset = StringIndexer(inputCol="Sex", outputCol="Gender", handleInvalid="keep").fit(dataset).transform(dataset)
dataset = StringIndexer(inputCol="Embarked", outputCol="Boarded", handleInvalid="keep").fit(dataset).transform(dataset)
dataset.show()

#Vector assembler:  assembles all values of all variables in an array of features
requred_features=[
    "Pclass",
    "Gender",
    "Age",
    "Fare",
    "Boarded"
]

assembler = VectorAssembler(inputCols=requred_features, outputCol="features")
transformed_data = assembler.transform(dataset)
transformed_data.show()



heart = spark.read.csv("data/heart.csv",header=True,inferSchema=True)
heart.printSchema()
heart.show()
assembler2=VectorAssembler(inputCols=["age","sex","cp","trestbps","chol","fbs","restecg","thalach","exang","oldpeak","slope","ca","thal"],outputCol="features")
transformed_data2=assembler2.transform(heart)
transformed_data2.show()

#Training the model
(training_data,test_data) = transformed_data.randomSplit([0.7,0.3])
rf = RandomForestClassifier(labelCol="Survived", featuresCol="features",maxDepth=4,numTrees=100,maxBins=32)
rf_model = rf.fit(training_data)
predictions = rf_model.transform(test_data)
predictions.show()

evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %g" % accuracy)





train,test=transformed_data2.randomSplit([0.7,0.3])
lr = LogisticRegression(labelCol="target",featuresCol="features")
lr_model = lr.fit(train)
predictions = lr_model.transform(test)
predictions.show()

predict_train=lr_model.transform(train)
predict_train.show()
predict_test=lr_model.transform(test)
predict_test.show()

evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
areaUnderROC = evaluator.evaluate(predict_train)
print("areaUnderROC = %g" % areaUnderROC)
areaUnderROC = evaluator.evaluate(predict_test)
print("areaUnderROC = %g" % areaUnderROC)