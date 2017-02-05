########################################### IMPORTS ####################################################
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

################################### STATISTICS ON FINAL RESULTS ########################################
############## ALL RESULTS ARE SAVED INTO PARQUET FILES AND USED IN visualization.ipynb ################

# Download result of sentiment prediction
load_final = sqlContext.read.parquet("hdfs:///user/prado/little_after_sentana.parquet")

# pos/neg/neutre count
pnn_pie = load_final.groupBy('label').count()
pnn_pie.write.save("hdfs:///user/prado/total_sent.parquet",mode = 'overwrite', format="parquet")

#pos/neg/neutre count per month
pnn_per_month = load_final.crosstab('label', 'date_found')
pnn_per_month.write.save("hdfs:///user/prado/total_sent_per_month.parquet",mode = 'overwrite', format="parquet")

# Tag count stats
a = load_final.select('tags').flatMap(lambda line: line[0]).map(lambda tag: (tag.lower(),1)).reduceByKey(lambda x,y: x+y)
tag_count = a.toDF()
stats_tags = tag_count.describe()
stats_tags.write.save("hdfs:///user/prado/tags_stats.parquet",mode = 'overwrite', format="parquet")

# Tag count for happy/neg/neutral instagrams
for i in [0, 1, 2]:
    #tag count
    text0 = load_final.filter(load_final.label==i).select('tags').flatMap(lambda line: line[0])
    text1 = text0.map(lambda tag: (tag.lower(),1)).reduceByKey(lambda x,y: x+y)
    
    # Keep 1000 more used tags
    first_rows = text1.toDF().sort(desc("_2")).limit(1000)
    first_rows.write.save("hdfs:///user/prado/tags_per_sent_"+ str(i) +".parquet", mode = 'overwrite', format = "parquet")

    