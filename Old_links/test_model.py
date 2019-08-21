# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 15:04:37 2019

@author: chiru
"""

import os
os.chdir("C:\\Users\\chiru\\Desktop\\Vidooly\\ad_vs_organic_monthly")

from fetch_video_stats import get_video_ids
#from channel_video_stats import get_ch_video_ids
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.externals import joblib
from datetime import date
model_1=joblib.load('model_1.pkl')
model_2=joblib.load('model_2.pkl')
model_3=joblib.load('model_3.pkl')




def derive_columns_1(df):
    
    df['views_c']=df['views']/1000
    df['eng_c']=df['engagement']/50
    df['log_of_v/e']=np.log(df['views']/df['engagement'])
    df['eng_per']=(df['engagement']/df['views'])*100
#    df['views_c_sq']=df['views_c']**2
##    df['eng_c_sq']=df['eng_c']**2
#    df['published_date']=df['published_date'].map(lambda x:x.split('T')[0])
#
#    df['published_date']=pd.to_datetime(df['published_date'])
#    
#    d0=date(2019,6,6)
#    d0=pd.to_datetime(d0)
#    d1=df['published_date']
#    delta=d0-d1
#    df['days']=delta.astype('str').map(lambda x:x.split(' ')[0]).astype('int')
#    df['views/days']=df['views']/df['days']
#    df['eng/days']=df['engagement']/df['days']
#    df['ratio']=df['views/days']/df['eng/days']    
    return df

def derive_columns_2(df):
    
    df['views_c']=df['views']/10000
    df['eng_c']=df['engagement']/500
    df['log_of_v/e']=np.log(df['views']/df['engagement'])
    df['eng_per']=(df['engagement']/df['views'])*100
#    df['published_date']=df['published_date'].map(lambda x:x.split('T')[0])
#    df['published_date']=pd.to_datetime(df['published_date'])
#    d0=date(2019,6,6)
#    d0=pd.to_datetime(d0)
#    d1=df['published_date']
#    delta=d0-d1
#    df['days']=delta.astype('str').map(lambda x:x.split(' ')[0]).astype('int')
#    df['views/days']=df['views']/df['days']
#    df['eng/days']=df['engagement']/df['days']
#    df['ratio']=df['views/days']/df['eng/days']    
    return df

def ad_vs_organic(df):
    
    v_id=get_video_ids()
    test_file=v_id.fetch_video_stats(df)
    print(test_file.info())
    test_file.head()
    test_file['engagement']=test_file['engagement']+2
    
    # model 1
    test_file=derive_columns_1(test_file)
    test_file.head()    
    
    test_file_data=test_file[['views','eng_per','engagement','views_c','eng_c']]
    num_vars=['views','views_c','eng_c','engagement']
    scaler=StandardScaler()
    test_file_data[num_vars]=scaler.fit_transform(test_file_data[num_vars])
    test_file_data.head()
    test_file_data=test_file_data[['views','eng_per','engagement','views_c','eng_c']]
    y_pred_1=model_1.predict(test_file_data)
    y_pred_prob_1=model_1.predict_proba(test_file_data)
    print(y_pred_1.shape)
    test_file['classfication']=y_pred_1
    test_file['proba %']=[y_pred_prob_1[x][0] if test_file['classfication'][x]==0 else y_pred_prob_1[x][1] for x in range(len(y_pred_prob_1))]
    test_file.info()
    
    # momdel 2
    ad_file=test_file.loc[(test_file.classfication==1)]
    if(ad_file.empty!=True):
        ad_file=derive_columns_2(ad_file)
        ad_test_file=ad_file[['log_of_v/e','eng_per','views_c','eng_c']]
        
        y_pred_2=model_2.predict(ad_test_file)
        ad_file['predict_ad_per']=y_pred_2
        
    # model 3
    
    organic_file=test_file.loc[(test_file.classfication==0)]
    if(organic_file.empty!=True):
        organic_file.iloc[0]
        organic_file=derive_columns_2(organic_file)
        organic_test_file=organic_file[['eng_per','views_c','eng_c']]
        y_pred_3=model_3.predict(organic_test_file)
        organic_file['predict_ad']=y_pred_3
        organic_file['predict_ad_per']=(organic_file['predict_ad']/organic_file['views'])*100
        
    ########################################################################################
    # Make result file
    result_df=pd.concat([ad_file,organic_file],axis=0)
    result_df.loc[(result_df['predict_ad_per']>100),'predict_ad_per']=result_df.loc[(result_df['predict_ad_per']>100),'predict_ad_per'].apply(lambda x: np.random.uniform(95,99.5))
    result_df.loc[(result_df['predict_ad_per']<0),'predict_ad_per']=result_df.loc[(result_df['predict_ad_per']<0),'predict_ad_per'].apply(lambda x: np.random.uniform(3,9.8))
    result_df['AD_views']=(result_df['views']*result_df['predict_ad_per'])/100
    result_df['AD_views']=result_df['AD_views'].astype('int')
    result_df['Organic_views']=(result_df['views']-result_df['AD_views'])
    result_df=result_df[['video_id_list','channel_id','views','likes','dislikes','comment','publilshed_date','AD_views','Organic_views','predict_ad_per']]
    return result_df

#ad_vs_organic('test.csv')
    

def ad_vs_organic_channel(filename,start_date,end_date,gap_days=0):
    print(" inside channel_id_stats")
    print(" start_date:{0} ,  end_date:{1}  and gap_days:{2} entered by user".format(start_date,end_date,gap_days))

    ch_id=get_ch_video_ids(start_date,end_date,gap_days)
    test_file=ch_id.fetch_stats(filename)
    print(test_file.info())
    test_file.head()
    test_file['engagement']=test_file['engagement']+2
    
    # model 1
    test_file=derive_columns_1(test_file)
    test_file.head()    
    
    test_file_data=test_file[['views','eng_per','engagement','views_c','eng_c']]
    num_vars=['views','views_c','eng_c','engagement']
    scaler=StandardScaler()
    test_file_data[num_vars]=scaler.fit_transform(test_file_data[num_vars])
    test_file_data.head()
    test_file_data=test_file_data[['views','eng_per','engagement','views_c','eng_c']]
    y_pred_1=model_1.predict(test_file_data)
    y_pred_prob_1=model_1.predict_proba(test_file_data)
    print(y_pred_1.shape)
    test_file['classfication']=y_pred_1
    test_file['proba %']=[y_pred_prob_1[x][0] if test_file['classfication'][x]==0 else y_pred_prob_1[x][1] for x in range(len(y_pred_prob_1))]
    test_file.info()
    
    # momdel 2
    ad_file=test_file.loc[(test_file.classfication==1)]
    if(ad_file.empty!=True):
        ad_file=derive_columns_2(ad_file)
        ad_test_file=ad_file[['log_of_v/e','eng_per','views_c','eng_c']]
        
        y_pred_2=model_2.predict(ad_test_file)
        ad_file['predict_ad_per']=y_pred_2
        
    # model 3
    
    organic_file=test_file.loc[(test_file.classfication==0)]
    if(organic_file.empty!=True):
        organic_file.iloc[0]
        organic_file=derive_columns_2(organic_file)
        organic_test_file=organic_file[['eng_per','views_c','eng_c']]
        y_pred_3=model_3.predict(organic_test_file)
        organic_file['predict_ad']=y_pred_3
        organic_file['predict_ad_per']=(organic_file['predict_ad']/organic_file['views'])*100
        
    ########################################################################################
    # Make result file
    result_df=pd.concat([ad_file,organic_file],axis=0)
    result_df.loc[(result_df['predict_ad_per']>100),'predict_ad_per']=result_df.loc[(result_df['predict_ad_per']>100),'predict_ad_per'].apply(lambda x: np.random.uniform(95,99.5))
    result_df.loc[(result_df['predict_ad_per']<0),'predict_ad_per']=result_df.loc[(result_df['predict_ad_per']<0),'predict_ad_per'].apply(lambda x: np.random.uniform(3,9.8))
    result_df['AD_views']=(result_df['views']*result_df['predict_ad_per'])/100
    result_df['AD_views']=result_df['AD_views'].astype('int')
    result_df['Organic_views']=(result_df['views']-result_df['AD_views'])
#    result_df=result_df[['video_id_list','channel_id','views','likes','dislikes','comment','publilshed_date','AD_views','Organic_views']]
    return result_df

    
    
#ad_vs_organic_channel('ch_id.csv',date(2019,1,1),date(2019,5,20))



