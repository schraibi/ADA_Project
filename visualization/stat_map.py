########################################### IMPORTS ####################################################
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

################################## STATISTICS ON FINAL RESULTS FOR MAPS ################################
############## ALL RESULTS ARE SAVED INTO PARQUET FILES AND USED IN visualization.ipynb ################

# Download the results of localization for each insta
final_sites = sqlContext.read.parquet("hdfs:///user/prado/final_localization.parquet")

# How many instagrams do we have by cantons ?
canton_map = final_sites.groupBy('canton').count()
canton_map.write.save("hdfs:///user/prado/insta_by_canton.parquet", mode = 'overwrite', format="parquet")

# MARKERS
# For each sentiment : 
for i in [0,1,2]:
    
    # Cross tab: number of instagram by canton and by month
    per_canton_month = final_sites.filter(final_sites['label']==i).crosstab('canton', 'date_found')
    per_canton_month.write.save("hdfs:///user/prado/per_canton_month_"+str(i)+".parquet",mode = 'overwrite', format="parquet")

#CHOROPLETH MAP
# number of happy insta & total insta by canton
nb_hapinsta = final_sites.filter(final_sites["label"]==0).groupBy('canton').count().withColumnRenamed('count', 'happy')
nb_neutrinsta = final_sites.filter(final_sites["label"]==1).groupBy('canton').count().withColumnRenamed('count', 'neutral')
nb_insta = final_sites.groupBy('canton').count().withColumnRenamed('count', 'total')
finalinsta = nb_hapinsta.join(nb_insta, on = 'canton')
FINALINSTA = finalinsta.join(nb_neutrinsta, on = 'canton') 
FINALINSTA.write.save("hdfs:///user/prado/insta_map_scores.parquet", mode = 'overwrite', format="parquet")

# CHOROPLETH MAP:
# Number of instagram per cities
city_map = final_sites.groupBy('city').count()
city_map.write.save("hdfs:///user/prado/insta_density.parquet", mode = 'overwrite', format="parquet")