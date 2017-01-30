from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

import pandas as pd
import numpy as np
#import langid
import re
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
import unicodedata
import json
from pandas.io.json import json_normalize
import pyspark
import pandas as pd
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import functions as F

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
    
after_process = sqlContext.read.parquet("hdfs:///user/prado/little_text_process1.parquet")
print('with sentiment ML TRAINING')
mlinterest = after_process.na.drop(subset=["sentiment"])
sent_value = udf(sentiment_values, IntegerType())
MLinterest = mlinterest.withColumn('label', sent_value(mlinterest.sentiment))
#prendre que ceux en anglais
MLINTEREST = MLinterest.filter(MLinterest.lang=="en")
MLINTEREST1 = MLINTEREST.withColumn("label", MLINTEREST.label.cast(DoubleType()))

#create pipeline
tokenizer = Tokenizer(inputCol="main", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
nb = NaiveBayes()
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, nb])
#train model
model = pipeline.fit(MLINTEREST1)

lang_udf = udf(lambda s: en_and_u(s), BooleanType())
print('without sentiment value ML prediction')
ml_topredict = after_process.filter(after_process.sentiment.isNull())
ml_topredict2 = ml_topredict.filter(lang_udf(ml_topredict.lang))
prediction = model.transform(ml_topredict2)
prediction = prediction.withColumnRenamed('prediction', 'label').select('main','label','date_found','tags')

print('we merge both')
final0= MLINTEREST1.select('main','label','date_found', 'tags')
final = prediction.unionAll(final0)
mois_num_udf = udf(mois_num, StringType())
final = final.withColumn('date_found', mois_num_udf(final['date_found']))

final.write.save("hdfs:///user/prado/little_after_sentana.parquet",mode = 'overwrite', format="parquet")