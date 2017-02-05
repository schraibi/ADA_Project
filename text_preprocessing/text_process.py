from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName("building a warehouse")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#import langid
import re
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
stops = stopwords.words("english")
import unicodedata
import pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.sql import functions as F

# REMOVE URLS    
def remove_urls(text):
    
    url_re = re.compile('(https?://)?(www.)?\w+\.\w+(/\w+)*/?')

    # remove hyperlinks
    text = url_re.sub(' ', text)

    return text

def _first_cleanzer(text):
    #remove to lower case
    
    text = text.lower()
    
    cleaned_text = ''
    text = text.split()
    at_re = re.compile('[@]')
    hash_re = re.compile('[#]')
    list_pos = 0
    for word in text:
        if at_re.search(word) == None and (hash_re.search(word) == None):
            # rebuild cleaned_text
            if list_pos == 0:
                cleaned_text = word
            else:
                cleaned_text = cleaned_text + ' ' + word
            list_pos += 1
            
    return str(cleaned_text)

def filter_text(text, stops):
    # Remove stop words
    single_characters = ['a', 'b','c', 'd','e','f','g', 'h', 'i', 'j', 'k', 'l', 
                        'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 're', 've', 'isn', 'th']
    perso_stopwords =  single_characters+stops          
    stops = set(perso_stopwords)
    list_pos = 0
    cleaned_text = ''
    text = text.split()
    
    # regex for emojis
    emoticons_str = '(\:\w+\:|\<[\/\\]?3|[\(\)\\\D|\*\$][\-\^]?[\:\;\=]|[\:\;\=B8][\-\^]?[3DOPp\@\$\*\\\)\(\/\|])(?=\s|[\!\.\?]|$)'
    
    # regex for words
    regex_str = [ emoticons_str,"(?:[a-z][a-z'\-_]+[a-z])"] 
    clean_re = re.compile('('+'|'.join(regex_str)+')')
    
    # regex for ':' at the end of a string only
    # To change 'look: here is my cat' in 'look here is my cat' 
    two_dots_re = re.compile('[:]$')
    
    # remove punctuation
    punc_re = re.compile('[.!?_,/]+')
    #text = punc_re.sub(' ', text)
    for word in text:
        
        if word not in stops and clean_re.match(word)!=None:
            # rebuild cleaned_text
            if list_pos == 0:
                cleaned_text = two_dots_re.sub('', punc_re.sub(' ', word))
            else:
                cleaned_text = cleaned_text + ' ' +two_dots_re.sub('',  punc_re.sub(' ', word))
            list_pos += 1
    
    if (len(cleaned_text)>7):
        
        return cleaned_text.replace("  ", " ")
    
    else:
        return ''

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


df = sqlContext.read.json('hdfs:///datasets/goodcitylife/*/harvest3r_instagram_data_*.json')
interest = df.select("_source.main", "_source.sentiment", "_source.lang", "_source.tags", "_source.date_found")

# TEXT PREPROCESSING
strip_accents_udf = udf(strip_accents, StringType())
remove_urls_udf = udf(remove_urls, StringType())
_first_clean_udf = udf(_first_cleanzer, StringType())
filter_text_udf = udf( lambda s: filter_text(s,stops), StringType())

interest1 = interest.withColumn('main', _first_clean_udf(interest.main))
interest2 = interest1.withColumn('main', strip_accents_udf(interest1.main))
interest3 = interest2.withColumn('main', remove_urls_udf(interest2.main))
interest4 = interest3.withColumn('main', filter_text_udf(interest3.main))
interest5 = interest4.filter(interest4.main!='')

# Save the text preprocessing
interest5.write.save("hdfs:///user/prado/little_text_process1.parquet",mode = 'overwrite', format="parquet")