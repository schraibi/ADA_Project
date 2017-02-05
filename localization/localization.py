########################################### IMPORTS ####################################################
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType

########################################### FUNCTIONS ##################################################
def create_city_id(df1):
    dictzer = dict()
    num_cities = []
    name_cities = []
    name_canton = []

    for i in np.arange(df1.shape[0]):
        new_str = df1.ix[i,:][0].split('\t')
        num_cities.append(new_str[3])
        name_canton.append(new_str[2])
        #remove all between ( .. ) + remove all whitespace
        nom1 = re.sub('\((.+)\)','',new_str[4].lower()).replace(" ", "")
        #remove all after /
        nom2 = re.sub('\/(.+)','', nom1)
        # remove all symbols
        nom3 = re.sub(r'[^\w]', '', nom2)
        #remove all a at the end of words ...
        name_cities.append(nom3)

    dictzer['name'] = np.asarray(name_cities)
    dictzer['id'] = np.asarray(num_cities)
    dictzer['canton'] = np.asarray(name_canton)
    dfwname = pd.DataFrame(data = dictzer)

    long_str=[]
    for i in np.arange(dfwname.shape[0]):
        long_str.append(len(dfwname['name'][i]))
    dfwname['len_name'] = long_str
    # remove 2 letters cities
    dfwname=dfwname[(dfwname['len_name']!=2)]
    # add cities by hand
    add_df = pd.DataFrame([['LU','1061', 'lucerne'], ['BE','584','jungfrau'],['BE','584','jungfraufoch'],['LU','1061', 'lucerna'],['GE','6621', 'geneva'], ['LU','1061', 'lucern'], ['OW','1402', 'Engelberg'],['VD','5586','igerslausanne'],['SH','2937','rheinfall'],['VD','5586','ouchy'],['GE','6621','igersgeneve'],['GE','6621','lakegeneva'],['GE','6621','igersgeneva'],['BS','2701','igersbasel'],['ZH','261','igerszurich'],['BE','351','igersbern'],['TI','5113','igerslocarno']], columns=['canton','id','name'])
    final_df = dfwname.append(add_df)
    final_df.loc[323,'id']='261'
    return final_df

def localise(tag_list, final_df):
    for tag in tag_list:
        if (final_df['name'] == tag.lower()).any():
            # on lui donne un ID
            return [final_df[final_df['name'] == tag.lower()].iloc[0,1], final_df[final_df['name'] == tag.lower()].iloc[0,0]]
    return ['-1','-1']

################################### SPARK FIND LOCALIZATION ############################################

# Definition of udf functions
canton_udf=udf(lambda s: s.split(',')[1][:-1],StringType())
city_udf=udf(lambda s: s.split(',')[0][1:],StringType())
localise_udf = udf(lambda s: localise(s, final_df), StringType())

# Downloading results of sentiment prediction
load_final = sqlContext.read.parquet("hdfs:///user/prado/little_after_sentana.parquet")

# From a text file, retrieve all municipality id
# All information in a pandas dataframe
df1 = pd.read_csv('liste_com.txt', sep='\r\t', header = None, engine ='python')
final_df = create_city_id(df1)

# Train the algorithm on the tag lists
localize = load_final.withColumn('site', localise_udf(load_final.tags))
localize1=localize.withColumn('canton',canton_udf(localize.site)).withColumn('city',city_udf(localize.site))

# Filter those where the algorithm failed
localize2 = localize1.filter(localize1.city!='-1')

# Saving results
localize2.write.save("hdfs:///user/prado/final_localization.parquet",mode = 'overwrite', format="parquet")