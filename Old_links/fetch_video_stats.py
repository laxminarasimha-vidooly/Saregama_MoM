#Importing libraries
import requests,json
import pprint
import pandas as pd
import numpy as np
import random
from datetime import date
from datetime import timedelta
import datetime
import operator 
import isodate
import os

# build class function
class get_video_ids:
    
    # functio to get new api key
    def get_new_key(self,api_key):
        self.api_key=api_key
        print('inside get_new_key func',self.api_key)
        self.exceed_key.append(self.api_key)
        self.api_key_list.remove(self.api_key)
        #print(api_key_list)
        if(len(self.api_key_list)):
            self.api_key=random.choice(self.api_key_list)
            return self.api_key
        else:
            print("key finished")
            return None

    # functin to get url and check status code of statistics of videos 
    def url_new(self,vo_id):
#        global api_key
        print("inside url_new func",self.api_key)
        url_1="https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,topicDetails&maxResults=50&key={0}&id={1}".format(self.api_key,vo_id)
        print(url_1)
        r=requests.get(url_1)
        
        while(r.status_code!=200):
            print("inside while loop",r.status_code)
            print("get new api key")
            new_key=self.get_new_key(self.api_key)
            print("new key is : ",new_key)
            if new_key:
                api_key=new_key
                url_1="https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,topicDetails&maxResults=50&key={0}&id={1}".format(api_key,vo_id)
                print("next url",url_1)
                r=requests.get(url_1)
            else:
                print(" key finished -> url_new func")
                break
        else:
            print("got status code success of url_new")
    
    
        return r


    def video_stats(self,r_dict_1):
        print('getting videos ')
#        global index
        
        for i in range(len(r_dict_1['items'])):
            self.df.loc[self.index,'video_id_list']=r_dict_1['items'][i].get('id','NA')
            self.df.loc[self.index,'video_title']=r_dict_1['items'][i]['snippet'].get('title','NA')
            self.df.loc[self.index,'channel_id']=r_dict_1['items'][i]['snippet'].get('channelId','NA')
            self.df.loc[self.index,'channel_title']=r_dict_1['items'][i]['snippet'].get('channelTitle','NA')
            if r_dict_1['items'][i].get('statistics'):
                self.df.loc[self.index,'views']=r_dict_1['items'][i]['statistics'].get('viewCount',0)
                self.df.loc[self.index,'likes']=r_dict_1['items'][i]['statistics'].get('likeCount',0)
                self.df.loc[self.index,'dislikes']=r_dict_1['items'][i]['statistics'].get('dislikeCount',0)
                self.df.loc[self.index,'comment']=r_dict_1['items'][i]['statistics'].get('commentCount',0)
            self.df.loc[self.index,'category']=r_dict_1['items'][i]['snippet'].get('categoryId',0)
            self.df.loc[self.index,'publilshed_date']=r_dict_1['items'][i]['snippet'].get('publishedAt','NA')
            if r_dict_1['items'][i].get('contentDetails'):
                self.df.loc[self.index,'YT_duration']=r_dict_1['items'][i]['contentDetails'].get('duration','NA')
            self.index+=1
        print("current_shape",self.df.shape)
       

    def __init__(self):
        columns=['video_id_list','channel_id','channel_title','video_title','category','publilshed_date','YT_duration','views','likes','dislikes','comment']
        self.df=pd.DataFrame(columns=columns)
        # listing api_keys
        self.api_key_list=["","AIzaSyDsMLnCTpmJJY0qd4rUHQGS3PiO9Tw9kEs",'AIzaSyC1ZpuaygN3plfgzH4I2oxrrE_KvJn9T54','AIzaSyCAvUODK49-fCM0UoPErb8yeRMsA_4XHpQ','AIzaSyAAbyrs8ofXHbyVDZV-u7DpH9c9IVAOok0']
        # exhausted keys are inserted into exceed key
        self.exceed_key=[]
        self.api_key=self.api_key_list[0]   
        self.index=0
        

# function to fecth videos stats from file 
    def fetch_video_stats(self,df):
        
        
        df_file=df.copy()
        print(df_file.shape)
        
        print(df_file.video_id)
        
#        df_file['video_id']=df_file['video_id'].astype('str')
        #Check for duplicates
        check=df_file.duplicated(subset=['video_id'],keep='first')
        df_file=df_file.loc[~(check)]
#        print(check)
        i=0
        for x in range(0,len(df_file.video_id),50):
            video=list(df_file.video_id[i:i+50])
            video=",".join(video)
            v_dict=json.loads(self.url_new(video).text)
            self.video_stats(v_dict)
            #print(video)
            i+=50
        self.df['views']=self.df['views'].astype(int)
        self.df['likes']=self.df['likes'].astype(int)
        self.df['dislikes']=self.df['dislikes'].astype(int)
        self.df['comment']=self.df['comment'].astype(int)
        self.df['engagement']=self.df['likes']+self.df['dislikes']+self.df['comment']
        self.df=self.df.loc[~self.df.isnull().any(axis=1)]
#        self.df.to_csv('video_stats.csv')
        return self.df
   
