# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 11:00:36 2019

@author: chiru
"""

import os
os.chdir("C:\\Users\\chiru\\Desktop\\Vidooly\\ad_vs_organic_monthly")

import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
import datetime
import test_model
from fetch_video_stats import get_video_ids


class MonthlySplit:
    def __init__(self,current_date):
        # initialize current date
        self.current_date=pd.to_datetime(current_date)
        print(self.current_date)
        
    def MonthlyAdSplit(self,df):
        self.df=df
        # define new column : video_pb_date , where it will change day to 1st of  video published date
        self.df['video_pb_date']=self.df['Published'].apply(lambda x : x.replace(day=1))
        #convert video_pb_date to datetime 
        self.df['video_pb_date']=pd.to_datetime(self.df['video_pb_date'])
        d0=self.current_date
        # new column days : to calculate the total days till now of the video
        self.df['days']=d0-self.df['video_pb_date']
        # set days to 60
        d1=timedelta(days=60)
        #condition 1
        # split dateframe to greater than 60 days 
        df_1=self.df.loc[(self.df['days']>d1)]
        # get video ids into another dataframe for updating video stats
        ft_df=pd.DataFrame(df_1['Video Id'])
        ft_df.columns=['video_id']
        #get video ids updated  stats 
        v_id=get_video_ids()
        test_file=v_id.fetch_video_stats(ft_df)
        #merging stats results with current df
        df_1=pd.merge(test_file[['video_id_list','views']],df_1,left_on='video_id_list',right_on='Video Id',how='inner')
        # updating stats 
        df_1['Total Views']=df_1.views
        df_1['Organic Views']=df_1['Total Views']-df_1['Ad Views']
        df_1['Ad Percent']=(df_1['Ad Views']/df_1['Total Views'])*100
        df_1[self.current_date]=0
        # dropping not required column
        df_1.drop(['video_id_list','views','days','video_pb_date'],axis=1,inplace=True)
        
        # condition 2 
        # make seperate dataframe whose published date is less than 60
        df_2=self.df.loc[(self.df['days']<=d1)]
        # now make ad prediction of videos id in df_2
        df_ad_pred=df_2[['Video Id']]
        df_ad_pred.columns=['video_id']
        df_ad_pred=test_model.ad_vs_organic(df_ad_pred)
        
        # merge ad prediction results with current data frame
        df_2=pd.merge(df_ad_pred[['video_id_list','views','AD_views','Organic_views']],df_2,left_on='video_id_list',right_on='Video Id',how='inner')
        
        ''' Logic for videos for videos published date is less than 60 days 
        Check for each video that 
        if predicted ad views is greater than existing ad views and Organic views is greater than current organic views
            then update total views , ad views and organic views
        otherwise 
            put current month ad views zero
            and update total views and organic views
            '''
        df_2[self.current_date]=0
        for x in df_2.index:
            if(df_2.loc[x,'AD_views']>df_2.loc[x,'Ad Views'] and df_2.loc[x,'Organic_views']>df_2.loc[x,'Organic Views']):
                df_2.loc[x,'Total Views']=df_2.loc[x,'views']
                df_2.loc[x,self.current_date]=df_2.loc[x,'AD_views']-df_2.loc[x,'Ad Views']
                df_2.loc[x,'Ad Views']=df_2.loc[x,'AD_views']
                df_2.loc[x,'Organic Views']=df_2.loc[x,'Organic_views']
            else:
                df_2.loc[x,self.current_date]=0
                df_2.loc[x,'Total Views']=df_2.loc[x,'views']
                df_2.loc[x,'Organic Views']=df_2.loc[x,'Total Views']-df_2.loc[x,'Ad Views']
        
        df_2['Ad Percent']=(df_2['Ad Views']/df_2['Total Views'])*100
        
        # dropping not required columns
        
        df_2.drop(['video_id_list','views','AD_views','Organic_views','days','video_pb_date'],axis=1,inplace=True)
        
        # concatinate both dataframe 
        
        df_AD_final=pd.concat([df_1,df_2],axis=0)
        
        # save file to excel 
#        df_AD_final.to_excel('df_AD_final_results_test_file.xlsx',sheet_name='AD_sheet')
        
        # pass df_AD_final to organic 
        return df_AD_final
        
    def MonthlyOrganicSplit(self,qw,df_AD_final):
        self.qw=qw
        qw=pd.merge(qw,df_AD_final[['Video Id','Total Views','Ad Percent','Ad Views','Organic Views']],on='Video Id',how='inner')
        # Organic Views_y is current months views
        # Organic views_x is the previous motnh views
        qw[self.current_date]=qw['Organic Views_y']-qw['Organic Views_x']
        #dropping not required columns
        cols=['Total Views_x','Ad Percent_x','Ad Views_x','Organic Views_x']
        qw.drop(cols,axis=1,inplace=True)
        #renaming columns 
        qw=qw.rename(columns={'Ad Views_y':'Ad Views','Total Views_y':'Total Views','Ad Percent_y':'Ad Percent','Organic Views_y':'Organic Views'})
        # columns to list
        columns=qw.columns.tolist()
        columns_1=['Video Id','Video Title','Channel id','Channel Title','Published','Total Views','Ad Percent','Ad Views','Organic Views']
        columns=[x for x in columns if x not in columns_1]
        columns=columns_1+columns
        # make dataframe 
        qw=qw[columns]
        
        # make it to the file
#        qw.to_excel('df_AD_final_results_test_file.xlsx',sheet_name='Organic')
        return qw
        
        
# class object 
# pass Month starting date which need to be added 

test_data=MonthlySplit(date(2019,6,1))
# read AD file
df=pd.read_excel('Saregama_Final_31May2019.xlsx')
# read Organic file
qw=pd.read_excel("Saregama_Organic.xlsx")
# get new month AD split
ad_df=test_data.MonthlyAdSplit(df)
# get new Month Organic split
qw=test_data.MonthlyOrganicSplit(qw,ad_df)
# write both dataframe to single file 
path='saregama_monthly_split.xlsx'
writer=pd.ExcelWriter(path,engine='xlsxwriter')
ad_df.to_excel(writer,sheet_name='Ad')
qw.to_excel(writer,sheet_name='Organic')
writer.save()
writer.close()

        
        