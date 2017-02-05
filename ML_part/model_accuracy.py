########################################### IMPORTS ####################################################
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics

########################################### FUNCTIONS ##################################################
def sentiment_values(sent):
    if(sent=='POSITIVE'):
        return 0
    elif(sent=='NEUTRAL'):
        return 1
    elif(sent=='NEGATIVE'):
        return 2
def mois_num(text): 
    return text.split('-')[1]

def en_and_u(s):
    if s=='en' or s=='U':
        return True
    else:
        return False

############################## SPARK ML PIPELINE + CALCULATING ACCURACY ################################
after_process = sqlContext.read.parquet("hdfs:///user/prado/little_text_process1.parquet")
mlinterest = after_process.na.drop(subset=["sentiment"])
sent_value = udf(sentiment_values, IntegerType())
MLinterest = mlinterest.withColumn('label', sent_value(mlinterest.sentiment))
MLINTEREST = MLinterest.filter(MLinterest.lang=="en")
MLINTEREST1 = MLINTEREST.withColumn("label", MLINTEREST.label.cast(DoubleType()))

# Create pipeline
tokenizer = Tokenizer(inputCol="main", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
nb = NaiveBayes()
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, nb])

# Separate train/test
train, test = MLINTEREST1.randomSplit([0.6, 0.4], 24)
train.cache()

# Train our model
model = pipeline.fit(train)

predictionAndLabels = model.transform(test.withColumnRenamed('label', 'true_label'))
wesh=predictionAndLabels.select('prediction', 'true_label').rdd
metrics = MulticlassMetrics(wesh)

#print precision, recall and F1-score for each label:

for i in [0.,1.,2.]:
    
    print('scores for', i, '\n')
    precision = metrics.precision(i)
    recall = metrics.recall(i)
    f1Score = metrics.fMeasure(i)
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score, '\n')

# RESULTS:
# score for positive prediction

#Precision = 0.9160483747830396
#Recall = 0.8400702987697716
#F1 Score = 0.876415749665348 

#scores for neutral prediction 

#Precision = 0.5280181705380709
#Recall = 0.6571996963589247
#F1 Score = 0.585568933850325 

#scores for negative prediction

#Precision = 0.5088619402985075
#Recall = 0.7101504767009994
#F1 Score = 0.5928873971070088 
    
    
    
    
    
# print confusion matrix  
print(metrics.confusionMatrix())
#
#DenseMatrix([[ 293970.,   43401.,   12564.],
#             [  20855.,   50214.,    5337.],
#             [   6086.,    1484.,   18547.]])

