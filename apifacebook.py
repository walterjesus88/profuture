#!/usr/bin/env python
# coding: utf-8

# In[9]:


#estoy en develop
#import boto3 
import sys
import os
import requests, json, datetime, csv, time
import random
from random import randint
from datetime import datetime,timedelta
from datetime import date
import pandas as pd
from collections import defaultdict
from pandas import ExcelWriter
#from pandas.io.json import json_normalize
from pandas import json_normalize
import pyodbc
import numpy as np
import time
import datetime


# In[10]:


pd.options.mode.chained_assignment = None

conn = pyodbc.connect(	
	'Driver={SQL Server Native Client 11.0};'
	                      'Server=WRIVERA-LP;'
	                      'Database=PROFUTURO.DW;'
	                      'Trusted_Connection=yes;'
)


# conn = pyodbc.connect(	
#   	'Driver={SQL Server Native Client 11.0};'
#   	                      'Server=DESKTOP-RL7HSS5;'
#   	                      'Database=IPE.DW;'
#   	                      'Trusted_Connection=no;'
#   	                      'uid=UserData;'
#                             'pwd=1P32020'
#                          )



#ayer = datetime.date(2020,10,30)
#ahora = datetime.date(2021,2,19)
#print(ayer)
#print(ahora)

ahora = datetime.datetime.utcnow() - datetime.timedelta(days=0)
print(ahora)

ayer = ahora - datetime.timedelta(days=180)
print(ayer.date())

unixtime1 = time.mktime(ayer.date().timetuple())
#unixtime1 = time.mktime(ayer.timetuple())
print(unixtime1)


unixtime2 = time.mktime(ahora.date().timetuple())
#unixtime2 = time.mktime(ahora.timetuple())
print(unixtime2)



import json
from dateutil.parser import parse
from pathlib import Path  # Python 3.6+ only
env_path = Path('.') / 'data.json'

class ApiFacebook:
    def __init__(self, cliente, access, cuenta):
        self.cliente = cliente
        self.access = access  
        self.cuenta = cuenta

    def get_inbox(self):
        Cliente = self.cliente
        access_token=self.access
        cuenta = self.cuenta

        base='https://graph.facebook.com/v12.0/'

        # url=base+cuenta+'/conversations?fields=link,messages{created_time,message,id}&access_token=' + str(access_token)

        url=base+cuenta+'/conversations?fields=message_count,updated_time,link,name,messages.limit(2000){created_time,message,from,attachments},subject,participants&access_token='+ str(access_token)
        print(url)
        request = requests.get(url).json()
        
        if 'data' in request: 
            cursor = conn.cursor()
            try:
                conn.autocommit = False
                a = 1
                while(True):
                    #try:
                        print('request')
                        print(request)
                        for datos in request['data']:
                            print('si hay data')
                            part = []
                    
                            for p in datos['participants']['data']:
                                if p['name'] != 'Profuturo AFP':
                                    print(p['name'])
                                    part.append({'name':p['name'],'email':p['email'],'id':p['id']})
                            
                            dt = parse(datos['updated_time']) - datetime.timedelta(hours=5)                     
                            try:
                                cursor.execute("INSERT INTO [PROFUTURO.DW].[dbo].[Informe.Facebook_inbox] (id,message_count,fecha,hora,link,participants_id,participants_name,participants_email) VALUES(?,?,?,?,?,?,?,?)",
                                    datos['id'],datos['message_count'],dt.date(),dt.time(),datos['link'],part[0]['id'],part[0]['name'],part[0]['email'])
                            except Exception as e:
                                print('se esta actualizando los mensajes')                            
                                cursor.execute("UPDATE [PROFUTURO.DW].[dbo].[Informe.Facebook_inbox] SET message_count = ?,fecha=?,hora=? WHERE id = ?", datos['message_count'],dt.date(),dt.time(),datos['id'])
                            i =0
                            for msg in datos['messages']['data']:
                                
                                i=i+1;
                                multimedia = []
                                im=''
                                if 'attachments' in msg:
                                    for m in msg['attachments']['data']:
                                        if 'image_data' in m:
                                         
                                            multimedia.append({'url':m['image_data']['url']})
                                        elif 'video_data' in m:
                                            multimedia.append({'url':m['video_data']['url']})
                                if multimedia==[]:
                                    im = ''  
                                else:
                                    im=multimedia[0]['url']
                                
                                dt_created = parse(msg['created_time']) - datetime.timedelta(hours=5)                             
                                try:
                                    cursor.execute("INSERT INTO [PROFUTURO.DW].[dbo].[Informe.Facebook_message] (id,created_time,fecha,hora,attachments,message,from_id,from_name,from_email,inbox_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
                                            msg['id'],msg['created_time'],dt_created.date(),dt_created.time(),im,msg['message'],msg['from']['id'],msg['from']['name'],msg['from']['email'],datos['id'])
                                except Exception as e:
                                    print('ya existe ese mensaje')
                                    #print(e)
                        
                        time.sleep(60)
                        if request['paging']['next']:
                            url = request['paging']['next']#.encode('utf-8')
                                            
                            print(url)  
                        #if url:                
                            request = requests.get(url).json()
                            print('request2')                          
                            print(request)                          
                            a+=1
                            if a>=100:
                                break
                                print(a)
                        else:
                            break
                    #except KeyError:
                        #print(KeyError)                 
                        #break
            except pyodbc.DatabaseError as err:
                conn.rollback()
            else:
                conn.commit()
            finally:
                conn.autocommit = True        
            cursor.close()

            print('termino esta -------------> carga ')      


    def get_procesar(self):
        Cliente = self.cliente
        access_token=self.access
        cuenta = self.cuenta

        base='https://graph.facebook.com/v15.0/'
        #access_token='EAAFLWQphDb4BAOfw4GP5Ol3ZCvL407twiKC5Edd4ugs8mhoM7kAv7Ma0HI10fSTCLrEZBEm1KHJZAvVfc6I8usXNqmhYKlcKMeBT7pEYXvtaGYgy7SYEYvNQO0svOIbZA9BgpOLK4p4iXfPRONiuyl2uF5SxxZCz9AoZBXKZACtuwZDZD'
        #url=base+cuenta+'conversations?fields=messages.limit(100){message,created_time,from}&limit=500&access_token=' + str(access_token)
        #url=base+cuenta+'?fields=posts{likes.summary(true),created_time,id,from,is_popular,is_published,picture,shares,story,story_tags,subscribed,message,comments{id,created_time,from,like_count,message,comments{id,from,created_time,like_count,message}}}&access_token=' + str(access_token)
        url=base+cuenta+'?fields=posts.since('+str(unixtime1)+').until('+str(unixtime2)+'){status_type,created_time,id,from,is_popular,is_published,picture,shares.limit(0).summary(true),story,story_tags,subscribed,permalink_url,message,comments.limit(0).summary(true),likes.limit(0).summary(true),full_picture,reactions.limit(0).summary(true)}&access_token=' + str(access_token)
        print('url 1---------->')   

        print(url)
        request = requests.get(url).json()

        if 'posts' in request:
            print('exists post')   
            data = requests.get(url).json()['posts']
            post = []
            comentarios = []
            a = 1
            while(True):
                try:
                    for datos in data['data']:
                        post.append(datos)
                        print('datos 1---------->')
                        print(datos)
                        # Attempt to make a request to the next page of data, if it exists.
                    #print(data['conversations']['paging']['next'])
                    #time.sleep(1)

                    url = data['paging']['next'].encode('utf-8')              
                    print('url    ---------->')
                    print(url)
                    data = requests.get(url).json()
                    #print(data)

                    a+=1
                except KeyError:
                    # When there are no more pages (['paging']['next']), break from the
                    # loop and end the script.
                    break
                #print(a)

            df_facebok_post = pd.DataFrame(post)
            print('---------------------------------------->')
            df_facebok_post
            #exit()
            history_facebook_post = pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,ayer,ahora),conn)
            #                                    and Fecha Between " % Cliente,conn)
            #history_facebook_post.shape


            df_facebok_post['id'] = df_facebok_post.id.apply(lambda x: x if not pd.isnull(x) else 0)
            df_facebok_post['from'] = df_facebok_post['from'].apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['message'] = df_facebok_post.message.apply(lambda x: x if not pd.isnull(x) else '')
            #df_facebok_post['story'] = df_facebok_post.story.apply(lambda x: x if not pd.isnull(x) else '')
            #df_facebok_post['story_tags'] = df_facebok_post.story_tags.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['permalink_url'] = df_facebok_post.permalink_url.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['status_type'] = df_facebok_post.status_type.apply(lambda x: x if not pd.isnull(x) else '')
            df_facebok_post['full_picture'] = df_facebok_post.full_picture.apply(lambda x: x if not pd.isnull(x) else '')

            print(df_facebok_post.shares)

            if not df_facebok_post.shares.empty:
                df_facebok_post['shares'] = df_facebok_post.shares.apply(lambda x: x if not pd.isnull(x) else {'count':'0'})
            else:
                df_facebok_post['shares'] = '0'

            if not df_facebok_post.likes.empty:
                df_facebok_post['likes'] = df_facebok_post.likes.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})
            else:
                df_facebok_post['likes'] = '0'

            if not df_facebok_post.likes.empty:   
                df_facebok_post['reactions'] = df_facebok_post.reactions.apply(lambda x: x if not pd.isnull(x) else {"summary": {"total_count": 0}})
            else:
                df_facebok_post['reactions'] = '0'

            def cod_postid(id):
                subguion = '_' 
                index_guion = id.index(subguion)    
                longitud= len(id) 
                cod_postid=int(id[index_guion+1:longitud])
                return cod_postid

            def quitar_guion(fecha):
                fecha = str(fecha)
                fechanew = fecha.replace('-', '')
                return fechanew

            df_facebok_post['cod_postid'] = df_facebok_post['id'].apply(cod_postid)
            df_facebok_post['Fecha_resta'] = pd.to_datetime(df_facebok_post['created_time']) - datetime.timedelta(hours=5)
            df_facebok_post['Fecha'] = pd.to_datetime(df_facebok_post['Fecha_resta']).dt.date
            df_facebok_post['Fecha']= df_facebok_post['Fecha'].apply(quitar_guion)
            df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['Fecha_resta']).dt.time
            df_facebok_post['Hora'] = df_facebok_post['Hora'].apply(str)

            #df_facebok_post['z'] = pd.to_datetime(df_facebok_post['created_time'])
            #df_facebok_post['Hora'] = df_facebok_post['z'].dt.tz_convert('US/Central').dt.time
            #df_facebok_post['Hora'] = pd.to_datetime(df_facebok_post['Hora'], format='%H:%M:%S').dt.time
            #df_facebok_post['sa'].dt.tz_convert('US/Pacific')
            #df_facebok_post['sa'] = pd.to_datetime(df_facebok_post['sa'], format='%H:%M:%S')

        
            df_facebok_post.isnull().sum()
            df_facebok_post.dtypes
            #Sirve para insertar datos en la tabla FACEBOOK_POST
            currDate = datetime.datetime.now()

            #Insertar en Post
            cursor = conn.cursor()  
            try:
                conn.autocommit = False
                for index,row in df_facebok_post.iterrows(): 
                    history_post_exist =  history_facebook_post[(history_facebook_post.cod_postid == row.cod_postid)]

                    if history_post_exist.empty:
                        try:
                            print(row.cod_postid)
                            cursor.execute("INSERT INTO [PROFUTURO.DW].[dbo].[Informe.Facebook_post] (id,cod_postid,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,ARCHIVO,Flag_Postexterno,full_picture,reactions) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               row.cod_postid,row.cod_postid,Cliente,row.message,'','Facebook',row.status_type,0,row['from']['name'],'','','',row.permalink_url,0,row.Fecha,row.Hora,row['shares']['count'],'','','','','','','','','','','','',row['from']['id'],'','',currDate,row.likes['summary']['total_count'],'','','',row.id,'','','Facebook-Api','1',row.full_picture,row.reactions['summary']['total_count'])

                        except Exception as e:
                            print('e')
                            print(e)
                    else:
                        print(row.cod_postid)

            except pyodbc.DatabaseError as err:
                conn.rollback()
            else:
                conn.commit()
            finally:
                conn.autocommit = True        
            cursor.close()

            df_comments = pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message','from','url'])

            for posts in post:
                #print(posts['id'])    
                #url_comments=base+posts['id']+'?fields=comments{id,created_time,from,like_count,message}&access_token=' + str(access_token)
                #url_comments=base+posts['id']+'?fields=comments.limit(100000){id,created_time,like_count,message,from{name,id,username,link}}&access_token=' + str(access_token)
                #?fields=from{name,id,username,link}
                url_comments=base+posts['id']+'?fields=comments.limit(10000){id,created_time,like_count,message,from,permalink_url}&access_token=' + str(access_token)

                #graph.facebook.com/3771930046172217_3780537211978167?fields=from{name,id,username,link}

                comentarios =[]
                requests_comments = requests.get(url_comments).json()
                print(url_comments)

                if 'comments' in requests_comments:

                    try:
                        data2 = requests.get(url_comments).json()['comments']
                 
                    except KeyError:
                        continue
                    a = 1
                    while(True):
                        try:
                            for datos in data2['data']:
                                comentarios.append(datos)
                            #print(comentarios)
                            #print(Conversations)
                            # Attempt to make a request to the next page of data, if it exists.
                            #print(data['conversations']['paging']['next'])
                            #time.sleep(1)

                            url = data2['paging']['next'].encode('utf-8')
                            # print(url)

                            data2 = requests.get(url).json()
                            #print(data)
                            a+=1
                        except KeyError:
                            # When there are no more pages (['paging']['next']), break from the
                            # loop and end the script.
                            break
                        print(a)
                df_comments = pd.concat ([df_comments, pd.DataFrame(json_normalize(comentarios))]) 

            print('df_comments --->')
            print(df_comments)

            if not df_comments.empty:
                #https://stackoverflow.com/questions/29152500/get-real-profile-url-from-facebook-graph-api-user
                df_comments['Nombre'] = df_comments['from.name']
                df_comments['UserID'] = df_comments['from.id']
                df_comments

                #history_facebook_post = pd.read_sql("select * from [IPE.DW].[dbo].[Informe.Facebook_post] where Cliente = '%s' and convert(date,[Fecha]) BETWEEN '%s' AND '%s'" %(Cliente,dateTime_ini,dateTime_fin),conn)
                history_facebook_comments = pd.read_sql("select * from [PROFUTURO.DW].[dbo].[Informe.Facebook_comments] where Cliente = '%s' " % Cliente,conn)
                history_facebook_comments.shape

                def cod_comment_id(id):
                    subguion = '_' 
                    index_guion = id.index(subguion)    
                    longitud= len(id)  
                    cod_comment_id=int(id[index_guion+1:longitud])
                    return cod_comment_id

                def cod_postid_comment(id):
                    subguion = '_' 
                    index_guion = id.index(subguion)
                    cod_postid_comment=int(id[0:index_guion])
                    return cod_postid_comment

                df_comments['cod_comment_id'] = df_comments['id'].apply(cod_comment_id)
                df_comments['cod_postid_comment'] = df_comments['id'].apply(cod_postid_comment)
                df_comments['Fecha_resta'] = pd.to_datetime(df_comments['created_time']) - datetime.timedelta(hours=5)
                df_comments['Fecha'] = pd.to_datetime(df_comments['Fecha_resta']).dt.date
                df_comments['Fecha']= df_comments['Fecha'].apply(quitar_guion)
                df_comments['Hora'] = pd.to_datetime(df_comments['Fecha_resta']).dt.time
                df_comments['Hora'] = df_comments['Hora'].apply(str)

                #Insertando en facebook comments 
                cursor = conn.cursor()
                try:
                    conn.autocommit = False
                    for index,row in df_comments.iterrows():
                        #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado

                        try:
                            print('vacio ' + str(row.cod_comment_id))
                            cursor.execute("INSERT INTO [PROFUTURO.DW].[dbo].[Informe.Facebook_comments] (id,cod_comment_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,postid_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                row.cod_comment_id,row.cod_comment_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','',row.permalink_url,0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,'','','',row.id,'','','Facebook-Api',row.cod_postid_comment)
                        except Exception as e:
                            print(e)

                except pyodbc.DatabaseError as err:
                    conn.rollback()
                else:
                    conn.commit()
                finally:
                    conn.autocommit = True
                cursor.close()
                print('termino comments')
                df_reply_comments= pd.DataFrame(columns=['created_time', 'id', 'like_count', 'message'])
                df_reply_comments

                for index, row in df_comments.iterrows(): 
                    url_comments=base+str(row.id)+'?fields=comments{id,created_time,like_count,message,from}&access_token=' + str(access_token)
                    comentarios =[]
                    print(url_comments)
                    try:
                        data3 = requests.get(url_comments).json()['comments']

                    except KeyError:
                        continue
                    a = 1
                    while(True):
                        try:
                            for datos in data3['data']:

                                datos['inreplyto'] = row.id
                                comentarios.append(datos)                
                                #print(comentarios)
                            #print(Conversations)
                            # Attempt to make a request to the next page of data, if it exists.
                            #print(data['conversations']['paging']['next'])
                            #time.sleep(1)
                            url = data3['paging']['next'].encode('utf-8')
                            data3 = requests.get(url).json()

                            a+=1
                        except KeyError:
                            # When there are no more pages (['paging']['next']), break from the
                            # loop and end the script.
                            break
                        print(a)

                    df_reply_comments = pd.concat ([df_reply_comments, pd.DataFrame(json_normalize(comentarios))])

                def cod_reply_id(id):    
                    subguion = '_' 
                    index_guion = id.index(subguion)    
                    longitud= len(id) 
                    cod_reply_id=int(id[index_guion+1:longitud])
                    return cod_reply_id

                def cod_commentid_replycomment(inreplyto):
                    subguion = '_' 
                    index_guion = inreplyto.index(subguion)    
                    longitud= len(inreplyto) 
                    cod_commentid_replycomment=int(inreplyto[index_guion+1:longitud])
                    print('----------------------------------->',cod_commentid_replycomment)
                    return cod_commentid_replycomment

                print('datos----',df_reply_comments)

                if not df_reply_comments.empty: 

                    df_reply_comments['cod_reply_id'] = df_reply_comments['id'].apply(cod_reply_id)
                    df_reply_comments['cod_commentid_replycomment'] = df_reply_comments['inreplyto'].apply(cod_commentid_replycomment)
                    df_reply_comments['Fecha_resta'] = pd.to_datetime(df_reply_comments['created_time']) - datetime.timedelta(hours=5)
                    df_reply_comments['Fecha'] = pd.to_datetime(df_reply_comments['Fecha_resta']).dt.date
                    df_reply_comments['Fecha']= df_reply_comments['Fecha'].apply(quitar_guion)
                    df_reply_comments['Hora'] = pd.to_datetime(df_reply_comments['Fecha_resta']).dt.time
                    df_reply_comments['Hora'] = df_reply_comments['Hora'].apply(str)

                    df_reply_comments['Nombre'] = df_reply_comments['from.name']
                    df_reply_comments['UserID'] = df_reply_comments['from.id']
                    df_reply_comments

                    #Insertando en facebook comments 
                    cursor = conn.cursor()
                    try:
                        conn.autocommit = False
                        for index,row in df_reply_comments.iterrows():
                                #Comprueba si existe la tabla facebook_comments el row, para evitar conflicto de duplicado
                            print(row.id)
                            try:
                                cursor.execute("INSERT INTO [PROFUTURO.DW].[dbo].[Informe.Facebook_reply_comments] (id,cod_reply_id,Cliente,Mensaje,Valoracion,Medio,Tipo,Followers,Nombre,Categoria1,Categoria2,Categoria3,URL,Reporte,Fecha,Hora,FBShares,Departamento,Distrito,Org,Sexo,Rango_edad,Influencia,Titulo,Tag1,Tag2,Tag3,Tag4,Tag5,UserID,screen_name,friends,created_at,FbLikes,inreplyto,inreplytouserid,inreplytousername,postid,Lat,Lng,Archivo,cod_comment_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    row.cod_reply_id,row.cod_reply_id,Cliente,row.message,'','Comment','','',row.Nombre,'','','','https://facebook.com' + str(row.id),0,row.Fecha,row.Hora,'0','','','','','','','','','','','','',row.UserID,'','',currDate,row.like_count,row.inreplyto,'','',row.id,'','','Facebook-Api',row.cod_commentid_replycomment)
                            except Exception as e:
                                        #print('e')
                                print(e)

                    except pyodbc.DatabaseError as err:
                        conn.rollback()
                    else:
                        conn.commit()
                    finally:
                        conn.autocommit = True
                    cursor.close()
        print('termino' + Cliente)
        return str('termino  ' + Cliente)


