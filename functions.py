import pandas as pd
import numpy as np
import langid
import re
import nltk
from nltk.corpus import stopwords
import unicodedata
import json
from pandas.io.json import json_normalize

# ENGLISH DETECTION
def check_english(text):
    predict_lang = langid.classify(text)
    if predict_lang[1] >= .5:
        language = predict_lang[0]
    else:
        language = 'NA'
    return language

# REMOVE INSTAGRAM NOISE
def first_clean(text):
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

# REMOVE STOP WORDS
def remove_stops(text):
    stops = stopwords.words("english")
    single_characters = ['a', 'b','c', 'd','e','f','g', 'h', 'i', 'j', 'k', 'l', 
                        'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 're', 've', 'isn', 'th']
    perso_stopwords =  single_characters+stops
                 
    stops = set(perso_stopwords)
    list_pos = 0
    cleaned_text = ''
    text = text.split()
    
    for word in text:
        if word not in stops:
            # rebuild cleaned_text
            if list_pos == 0:
                cleaned_text = word
            else:
                cleaned_text = cleaned_text + ' ' + word
            list_pos += 1
    
    if (len(cleaned_text)>7):
        
        return cleaned_text
    
    else:
        return ''
    
# REMOVE ALL SYMBOLS    
def remove_features(text):
    
    url_re = re.compile('(https?://)?(www.)?\w+\.\w+(/\w+)*/?')
    punc_re = re.compile('[^a-z.!?]+')
    # remove hyperlinks
    text = url_re.sub(' ', text)
    # remove digits and all punctuation expect . ! and ?
    text = punc_re.sub(' ', text)
    return text

#REMOVE ACCENTS    
def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

# CREATE DATAFRAME WITH ALL MUNICIPALITIES AND CANTONS BASED ON a .txt file
def create_city_id():
    df1 = pd.read_csv('liste_com.txt', sep='\r\t', header = None, engine ='python')
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
        #nom4 = re.sub('a$', '', nom3)
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
    
    add_df = pd.DataFrame([['LU','1061', 'lucerne'], ['BE','584','jungfrau'],['BE','584','jungfraufoch'],['LU','1061', 'lucerna'],['GE','6621', 'geneva'], ['LU','1061', 'lucern'], ['OW','1402', 'Engelberg'],['VD','5586','igerslausanne'],['SH','2937','rheinfall'],['VD','5586','ouchy'],['GE','6621','igersgeneve'],['GE','6621','lakegeneva'],['GE','6621','igersgeneva'],['BS','2701','igersbasel'],['ZH','261','igerszurich'],['BE','351','igersbern'],['TI','5113','igerslocarno']], columns=['canton','id','name'])
    final_df = dfwname.append(add_df)
    final_df.loc[323,'id']='261'
    return final_df

# ALGORITHM THAT TRY TO LOCALISE A INSTA BASED ON THE TAG LIST
def localise(tag_list, final_df):
    for tag in tag_list:
        print(tag)
        if (final_df['name'] == tag.lower()).any():
            # on lui donne un ID
            return [final_df[final_df['name'] == tag.lower()].iloc[0,1], final_df[final_df['name'] == tag.lower()].iloc[0,0]]
    return ['-1','-1']

# CORRESPONDANCE BETWEEN SENTIMENT AND NUMERICAL VALUE
def sentiment_values(sent):
    if(sent=='POSITIVE'):
        return 0
    elif(sent=='NEUTRAL'):
        return 1
    elif(sent=='NEGATIVE'):
        return 2
# TO FIND THE MONTH GIVING THE INSTAGRAM 'date_found' feature 
def mois_num(text): 
    return text.split('-')[1]
