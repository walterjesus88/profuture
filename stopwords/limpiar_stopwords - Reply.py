#!/usr/bin/env python
# coding: utf-8

# In[2]:


from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
from collections import Counter
from _collections import OrderedDict

import numpy as np
#!/usr/bin/env python
# coding: utf-8
import pyodbc
import re
import pandas as pd
import json


conn = pyodbc.connect(	
	'Driver={SQL Server Native Client 11.0};'
	                      'Server=DESKTOP-RL7HSS5;'
	                      'Database=PROFUTURO.DW;'
	                      'Trusted_Connection=yes;'
	                      'uid=UserData;'
                      'pwd=1P32020')




xdata =pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] ",conn)
xdata.head(2)




def clean_text(text):
    text = re.sub(r'^RT[\s]+', '', text)
    text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r':', '', text)
    #text = re.sub(r':", "", text)
    text = re.sub(r'@[A-Za-z0-9]+', '', text)
    #print(text)
    text = text.lower().strip()
    text = text.translate(str.maketrans('','',string.punctuation))
    return text


xdata['Mensaje'] = xdata['Mensaje'].apply(str)
xdata['Mensaje_nuevo'] = xdata['Mensaje'].apply(clean_text)
xdata =xdata[xdata['Mensaje_nuevo'].str.len()>3]
#datos = datos[['cod_postid','Mensaje']]
xdata = xdata[['Mensaje','cod_reply_id','Mensaje_nuevo']]
xdata


def words_frecuency(texto):
    kv = []
    stop_words = set(stopwords.words('spanish'))
    word_tokens = word_tokenize(texto)

    word_tokens = list(filter(lambda token: token not in string.punctuation,word_tokens))
    filtro = ""

    for palabra in word_tokens:
        if palabra not in stop_words:
            filtro=filtro + " " + palabra
    #print(stop_words)        
    #print(word_tokens)
    #print(filtro)
    #ff = type(filtro)
    c = Counter(filtro)
    cc = c.most_common(6)
    y = OrderedDict(cc)
    for k,v in y.items():
        kv.append({k,v})
    return filtro
    #return k

#stop_words = set(stopwords.words('spanish'))
#print(stop_words)


def formatear(strings):
    tildes = ['á','é','í','ó','ú']
    vocales = ['a','e','i','o','u']

    # tildes
    for idx, vocal in enumerate(vocales):
        strings = strings.str.replace(tildes[idx],vocal)

    # caracteres especiales menos la ñ
    strings = strings.str.replace('[^a-zñA-Z ]', "")

    # todo a minusculas
    strings = pd.Series(list(map(lambda x: x.lower(), strings)))
    
    return strings

#stop_words_es = np.genfromtxt('stop_words_es.txt', dtype='str')
#stop_words_es = formatear(pd.Series(stop_words_es))
#stop_words_es = list(map(lambda x: x, stop_words_es))
#stop_words_es = set(stop_words_es)
#stop_words_es

xdata['filtro_stopword']=xdata['Mensaje_nuevo'].apply(words_frecuency)
xdata.tail()


def new_format(texto):   
    #stop_words = set(stopwords.words('spanish'))  
   # stop_words = open("stop_words_es.txt")  
    #print(texto)
    sum = ""
    for t in texto:
        sum= sum+" "+t
    #print(sum)
    filtro = ""
    
    stop_words_es = np.genfromtxt('stop_words_es.txt', dtype='str')
    stop_words_es = formatear(pd.Series(stop_words_es))

    stop_words_es = set(stop_words_es)
    stop_words_es

    # Use this to read file content as a stream:  
    #line = file1.read() 
    #words = line.split() 
    
    word_tokens = word_tokenize(texto)
    
    for palabra in word_tokens:
        if palabra not in stop_words_es:
            filtro= filtro + " "+ palabra
          
 
    return filtro

xdata['filtro_second']= xdata['filtro_stopword'].apply(new_format)
xdata



#xdata.to_csv("total.csv")

cursor = conn.cursor()
for index, row in xdata.iterrows():
    print(row.cod_reply_id)
    cursor.execute("UPDATE [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] SET Mensaje_clean = ? WHERE cod_reply_id = ?", row.filtro_second, row.cod_reply_id)
    cursor.commit()
cursor.close()







