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

########################################### FUNCTIONS ##################################################

# Give label to sentiments for classification task
def sentiment_values(sent):
    if(sent=='POSITIVE'):
        return 0
    elif(sent=='NEUTRAL'):
        return 1
    elif(sent=='NEGATIVE'):
        return 2
    
# Extract the month given the date
def mois_num(text): 
    return text.split('-')[1]

# Filter by language
def en_and_u(s):
    if s=='en' or s=='U':
        return True
    else:
        return False

####################################### SPARK ML PIPELINE ##############################################

# definition of udf functions
sent_value = udf(sentiment_values, IntegerType())
lang_udf = udf(lambda s: en_and_u(s), BooleanType())
mois_num_udf = udf(mois_num, StringType())

# Download text pre-processing results
after_process = sqlContext.read.parquet("hdfs:///user/prado/little_text_process1.parquet")

# Extracting Train set (where we have a sentiment value)
mlinterest = after_process.na.drop(subset=["sentiment"])

# Labelization
MLinterest = mlinterest.withColumn('label', sent_value(mlinterest.sentiment))

# Keep only those in english
MLINTEREST = MLinterest.filter(MLinterest.lang=="en")
MLINTEREST1 = MLINTEREST.withColumn("label", MLINTEREST.label.cast(DoubleType()))

# Create ML pipeline using Naive Bayes classifier
tokenizer = Tokenizer(inputCol="main", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
nb = NaiveBayes()
pipeline = Pipeline(stages=[tokenizer, hashingTF, idf, nb])

# Train the model on the train set
model = pipeline.fit(MLINTEREST1)

# Extracting the prediction set (where we don't have sentiment value)
ml_topredict = after_process.filter(after_process.sentiment.isNull())
ml_topredict2 = ml_topredict.filter(lang_udf(ml_topredict.lang))

# Prediction
prediction = model.transform(ml_topredict2)
prediction = prediction.withColumnRenamed('prediction', 'label').select('main','label','date_found','tags')

# Gather both prediction and train set in one dataframe and save it.
final0= MLINTEREST1.select('main','label','date_found', 'tags')
final = prediction.unionAll(final0)
final = final.withColumn('date_found', mois_num_udf(final['date_found']))
final.write.save("hdfs:///user/prado/little_after_sentana.parquet",mode = 'overwrite', format="parquet")