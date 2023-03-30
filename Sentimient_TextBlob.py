#!/usr/bin/env python
# coding: utf-8

# In[1]:


#conda install -c conda-forge textblob
#!pip3 install -U textblob


# In[2]:


#!python -m textblob.download_corpora


# In[1]:


# -*- coding: utf-8 -*-
# -*- coding: 850 -*-
import pyodbc
import pandas as pd
from textblob import TextBlob

import csv
import re
import time
import string


# In[2]:


#df= pd.read_csv("datos_prueba_sentiment.csv")
#df['Mensaje']
print('conexion a base de datos')


# In[3]:




#CONEXION A LA BASE DE DATOS
conn = pyodbc.connect(  
    'Driver={SQL Server Native Client 11.0};'
                          'Server=WRIVERA-LP;'
                          'Database=PROFUTURO.DW;'
                          'Trusted_Connection=yes;'
                          'uid=UserData;'
                          'pwd=1P32020')

# In[6]:


#CONSULTAMOS CON UN INNERJ JOIN TODOS LOS COMENTARIOS CON SUS RESPECTIVOS POST'S

hfacebook_comments =pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_Comments] WHERE  TextBlobSENTIMENT IS NULL and len(Mensaje)>3 ",conn)
#hfacebook_comments = hfacebook_comments.head(1000)

hfacebook_comments.shape


# In[33]:


#hfacebook_comments = hfacebook_comments.head(10)
hfacebook_comments


# In[34]:


#FUNCION PARA LIMPIAR TEXTO de # o RT @ #d = clean_text('hola # .haber  @jes a todos')
def clean_text(text):
    text = re.sub(r'^RT[\s]+', '', text)
    text = re.sub(r'https?:\/\/.*[\r\n]*', '', text)
    text = re.sub(r'#', '', text)
    text = re.sub(r'@[A-Za-z0-9]+', '', text)
    text = text.lower().strip()
    text = text.translate(str.maketrans('','',string.punctuation))
    return text


# In[35]:


def formatear(strings):
    tildes = ['á','é','í','ó','ú']
    vocales = ['a','e','i','o','u']

    # tildes
    for idx, vocal in enumerate(vocales):
        strings = strings.str.replace(tildes[idx],vocal)

    # caracteres especiales menos la ñ
    strings = strings.str.replace('[^a-zñA-Z ]', "")

    # todo a minusculas
    strings = pd.Series(list(map(lambda x: x.lower(), strings)),dtype='object')  
    
    return strings


# In[36]:


print('funcion de rango')
#FUNCION PARA CATEGORIZAR EL SENTIMIENTO DE ACUERDO A LA POLARIDAD DEL TEXTO
def x_range(x):
    if x > 0:
        return 'Positivo'
    elif x == 0:
        return 'Neutro'
    else:
        return 'Negativo'


# In[37]:


#REEMPLAZANDO UNA JERGA POR SU EQUIVALENTE EN ESPANIOL
def reemplazar(text):
    for index, row in df_jergas.iterrows():
        #print(row.significado)
        text = re.sub(row.palabras, row.significado, text)
        #print(text)
    return text


# In[38]:


print('Importando Jergas')
#LLAMAMOS A JERGAS CSV
df_jergas = pd.read_csv("jergas.csv",encoding = "utf-8",error_bad_lines=False,low_memory=False)
df_jergas


# In[39]:


#LIMPIAMOS Y VERIFICAMOS QUE EL TEXTO TENGA MAS DE 3 CARACTERES, POR QUE HAY COMENTARIOS CON IMAGEN O GIFS
pd.options.mode.chained_assignment = None  # default='warn'
print('clean')
datosfilter=hfacebook_comments
datosfilter['clean_texto'] = datosfilter['Mensaje'].apply(str)
#datosfilter['clean_texto'] = datosfilter['clean_texto'].apply(clean_text)
datosfilter['clean_texto'] = formatear(datosfilter['clean_texto'])

datosfilter =datosfilter[datosfilter['clean_texto'].str.len()>3]
datosfilter
print('datosfilter')


# In[40]:


#REEMPLAZAMOS EL TEXTO
datosfilter['clean_texto'] = datosfilter['clean_texto'].apply(reemplazar)
datosfilter.shape


# In[ ]:


print('iniciando la traduccion de comments')


# In[41]:


#FUNCION DE POLARIDAD Y TRADUCTOR
def get_polarity(text):
    analysis = TextBlob(text)
    if text != '':
        #if analysis.detect_language() == 'es':
        try:
            result = analysis.translate(from_lang = 'es', to = 'en').sentiment.polarity
            #result = analysis.sentiment.polarity
            print(result)
        except:
            print("An exception occurred")          
            result = 0
        time.sleep(1)
        return result


# In[42]:


print('traduciendo y extraendo polaridad de comments')
#APLICAMOS POLARIDAD 
#datosfilter['English']= datosfilter['clean_texto'].apply(translator.translate, src='es', dest='en').apply(getattr, args=('text',))
datosfilter['polarity'] = datosfilter['clean_texto'].apply(get_polarity)
#datosfilter['polarity'] = datosfilter['English'].apply(lambda x: TextBlob(x).sentiment.polarity)
datosfilter['polarity']


# In[44]:


datosfilter.shape


# In[45]:


print('aplicando rangos')
#APLICAMOS LOS RANGOS 
datosfilter['result'] = datosfilter['polarity'].apply(x_range)


# In[46]:


datosfilter.shape


# In[47]:


datosfilter = datosfilter.sort_values(by=['result'])


# In[48]:


#RESULTADOS CONTEO
datosfilter['result'].value_counts()


# In[49]:


datosfilter =datosfilter[['cod_comment_id','Mensaje','clean_texto','result']]


# In[50]:


#GUARDAMOS EN UN CSV DE SER NECESARIO
#datosfilter.to_excel('textblob_comments.xlsx')


# In[ ]:


print('actualizando tabla coments campo TextblobSENTIMENT')


# In[52]:


# -*- coding: 850 -*-
import pyodbc
cursor = conn.cursor()
for index, row in datosfilter.iterrows():
    print(row.result)
    cursor.execute("UPDATE [PROFUTURO.DW].[dbo].[Informe.Facebook_comments] SET TextBlobSENTIMENT = ? WHERE cod_comment_id = ?", row.result, row.cod_comment_id)
    cursor.commit()
cursor.close()


# In[53]:


print('extraendo sentimiento de reply comments')


# In[61]:


#CONSULTAMOS CON UN INNERJ JOIN TODOS LOS COMENTARIOS CON SUS RESPECTIVOS POST'S
#replycomments = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_reply_comments]  where convert(date,[Fecha]) BETWEEN '%s' AND '%s'"%(fecha_me, fecha_ma),conn)
replycomments = pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments]  where  TextBlobSENTIMENT IS NULL ",conn)
replycomments.shape


# In[62]:


replycomments.head(81)


# In[63]:


print('reply comments polaridad y traduccion')


# In[64]:




replycomments['clean_texto'] = replycomments['Mensaje'].apply(str)
#datosfilter['clean_texto'] = datosfilter['clean_texto'].apply(clean_text)
replycomments['clean_texto'] = formatear(replycomments['clean_texto'])
replycomments =replycomments[replycomments['clean_texto'].str.len()>3]
replycomments['clean_texto'] = replycomments['clean_texto'].apply(reemplazar)
replycomments.shape              


# In[65]:


replycomments


# In[66]:


print('APLICAMOS POLARIDAD ')
#APLICAMOS POLARIDAD 
replycomments['polarity'] = replycomments['clean_texto'].apply(get_polarity)
replycomments['polarity']


# In[67]:


print('APLICAMOS LOS RANGOS ')
#APLICAMOS LOS RANGOS 
replycomments['result'] = replycomments['polarity'].apply(x_range)


# In[68]:


replycomments = replycomments.sort_values(by=['result'])


# In[69]:


#RESULTADOS CONTEO
replycomments['result'].value_counts()


# In[70]:


replycomments =replycomments[['cod_reply_id','Mensaje','clean_texto','result']]


# In[71]:


#GUARDAMOS EN UN CSV DE SER NECESARIO
#replycomments.to_excel('textblob_reply.xlsx')


# In[ ]:


print('actualizando tabla reply coments campo NTLKSENTIMENT')


# In[72]:


cursor = conn.cursor()
for index, row in replycomments.iterrows():
    print(row.result)
    cursor.execute("UPDATE [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] SET TextBlobSENTIMENT = ? WHERE cod_reply_id = ?", row.result, row.cod_reply_id)
    cursor.commit()
cursor.close()


# In[ ]:


print('consultando tabla sentiment POST')


# In[85]:


#CONSULTAMOS CON UN INNERJ JOIN TODOS LOS COMENTARIOS CON SUS RESPECTIVOS POST'S
#post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(fecha_me, fecha_ma),conn)
post = pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_post] where TextBlobSENTIMENT IS NULL ",conn)
post.shape


# In[89]:


post.head()


# In[90]:



post['clean_texto'] = post['Mensaje'].apply(str)
#datosfilter['clean_texto'] = datosfilter['clean_texto'].apply(clean_text)
post['clean_texto'] = formatear(post['clean_texto'])
post =post[post['clean_texto'].str.len()>3]
post['clean_texto'] = post['clean_texto'].apply(reemplazar)
post.shape 
                  


# In[94]:


print('APLICAMOS POLARIDAD Y TRADUCCION ')
#APLICAMOS POLARIDAD 
post['polarity'] = post['clean_texto'].apply(get_polarity)
post['polarity']


# In[95]:


#APLICAMOS LOS RANGOS 
post['result'] = post['polarity'].apply(x_range)


# In[96]:


post = post.sort_values(by=['result'])


# In[97]:


#RESULTADOS CONTEO
post['result'].value_counts()


# In[98]:


post =post[['cod_postid','Mensaje','clean_texto','result']]


# In[99]:


#GUARDAMOS EN UN CSV DE SER NECESARIO
#post.to_excel('textblob_post.xlsx')


# In[ ]:


print('actualizamos la tabla post campo ntlksentiment')


# In[100]:


cursor = conn.cursor()
for index, row in post.iterrows():
    print(row.result)
    cursor.execute("UPDATE [PROFUTURO.DW].[dbo].[Informe.Facebook_post] SET TextBlobSENTIMENT = ? WHERE cod_postid = ?", row.result, row.cod_postid)
    cursor.commit()
cursor.close()


# In[ ]:


print('Actualizaciones realizadas ... carga terminada')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




