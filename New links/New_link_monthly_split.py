
#Importing libraries
import os
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
import datetime
import test_model
from fetch_video_stats import get_video_ids
import random

#Preparing dataset
class New_link_MonthlySplit:
    def get_ADprediction(self,df):
        return test_model.ad_vs_organic(df)
    
    def __init__(self,current_date):
        # initialize current date
        self.current_date=pd.to_datetime(current_date)
        print(self.current_date)
        
        # function to get next month 
    def next_month(self,dt):
        d2=timedelta(days=40)
        dt=dt+d2
        dt=dt.replace(day=1)
        return dt
    #Ad monthly split
    def AD_Monthly_split(self,df):
        self.df=self.get_ADprediction(df)
        # convert published date to datetime
        self.df['publilshed_date']=self.df['publilshed_date'].apply(lambda x : x.split('T')[0])
        self.df['publilshed_date']=pd.to_datetime(self.df['publilshed_date'])
        print(self.df['publilshed_date'].min())
        # new column :get month first date of published date
        self.df['pb_date']=self.df['publilshed_date'].apply(lambda x : x.replace(day=1))
        # Make new columns from video min published date to current date
        cols=pd.date_range(self.df['pb_date'].min(),self.current_date,freq='MS').tolist()
        for x in cols:
            self.df[str(x)]=np.nan
        
        d0=self.current_date
        d0=pd.to_datetime(d0)
        # calculate the total days till now of the video
        self.df['days']=d0-self.df['pb_date']
        # set days to 60
#        d1=timedelta(days=60)
#        d1
        df_1=self.df.copy()
        
        #Processing
        for x in df_1.index:
            if (df_1.loc[x,'pb_date']!=self.current_date):
                # condition to check that published date of video lies between 1st-10ths 
                if( df_1.loc[x,'publilshed_date'].day >=1 and df_1.loc[x,'publilshed_date'].day<=10):
                    pb_date=df_1.loc[x,'publilshed_date']
                    #         print(pb_date)
                    pb_date=pb_date.replace(day=1)
                    # print(pb_date)
                    # 95 % percent of ad is stored in that month
                    per_split=random.uniform(.87,.97)
                    df_1.loc[x,str(pb_date)]=int(df_1.loc[x,'AD_views']*per_split)
    #             print(df_1.loc[x,[str(pb_date),'video_id_list','AD_views']])
                    # get next month date
                    pb_date_next=self.next_month(pb_date)
                    #remaining ad views are stored in next month
                    df_1.loc[x,str(pb_date_next)]=df_1.loc[x,'AD_views']-df_1.loc[x,str(pb_date)]

                elif(df_1.loc[x,'publilshed_date'].day >10 and df_1.loc[x,'publilshed_date'].day<=20):
                    pb_date=df_1.loc[x,'publilshed_date']
    #                 print(pb_date)
                    pb_date=pb_date.replace(day=1)
    #                 print(pb_date)
                    # 95 % percent of ad is stored in that month
                    per_split=random.uniform(.72,.82)
                    df_1.loc[x,str(pb_date)]=int(df_1.loc[x,'AD_views']*per_split)
    #               print(df_1.loc[x,[str(pb_date),'video_id_list','AD_views']])
                # get next month date
                    pb_date_next=self.next_month(pb_date)
                    #remaining ad views are stored in next month
                    df_1.loc[x,str(pb_date_next)]=df_1.loc[x,'AD_views']-df_1.loc[x,str(pb_date)]

                else:
                    pb_date=df_1.loc[x,'publilshed_date']
    #                 print(pb_date)
                    pb_date=pb_date.replace(day=1)
    #             print(pb_date)
                # 95 % percent of ad is stored in that month
                    per_split=random.uniform(.62,.72)
                    df_1.loc[x,str(pb_date)]=int(df_1.loc[x,'AD_views']*per_split)
    #             print(df_1.loc[x,[str(pb_date),'video_id_list','AD_views']])
                    # get next month date
                    pb_date_next=self.next_month(pb_date)
                    #remaining ad views are stored in next month
                    df_1.loc[x,str(pb_date_next)]=df_1.loc[x,'AD_views']-df_1.loc[x,str(pb_date)]
            else:
                print(df_1.loc[x,'pb_date'],df_1.loc[x,'publilshed_date'])
                pb_date=df_1.loc[x,'publilshed_date']
                pb_date=pb_date.replace(day=1)
                df_1.loc[x,str(pb_date)]=int(df_1.loc[x,'AD_views'])

        
        print(df_1.columns)
        df_1.drop(['pb_date','days'],axis=1,inplace=True)
        return df_1
    # get number of months between published date and current date
    

    #Daily tracking sheet processing
    def daily_tracking(self,df):
        df['percentage_change']=(df['viewchange']/df['views'])*100
        df['new_created_date']=pd.to_datetime(df['created_at']).apply(lambda x :x.replace(day=1))
        df_1=df.groupby(['videoid','published_at','new_created_date'])
        df_2=pd.DataFrame(df_1['viewchange'].sum())
        df_2['views']=df_1['views'].max()
        df_2['percentage']=(df_2['viewchange']/df_2['views'])*100
        
        return df_2
    #Organic monthly split
    def Organic_monthly_split(self,df_1,df_2):
        # df_1 is daily tracking 
        #df_2 is AD split 
        print(df_1.shape)
        print(df_2.shape)
        # from daily tracking data get viewchange for current date
        df_1['new_created_date']=pd.to_datetime(df_1['new_created_date'],dayfirst=True)
        df_1=df_1.loc[df_1['new_created_date']==self.current_date]
        # merge daily tracking and AD split dataframe
        df_3=pd.merge(df_1[['videoid','percentage','new_created_date']],df_2[['video_id_list','channel_id','views','likes','dislikes','comment','publilshed_date','AD_views','Organic_views']],
             left_on='videoid',right_on='video_id_list',how='inner')
        df_3.publilshed_date=pd.to_datetime(df_3.publilshed_date,dayfirst=True)

        print(df_3.publilshed_date.min())
        # make columns for each month with default Nan values
        cols=pd.date_range(df_3['publilshed_date'].min(),self.current_date,freq='MS').tolist()

        for x in cols:
            df_3[str(x)]=np.nan
        # make new column form publiished date and replace day to month starting date
        df_3['pb_date']=df_3['publilshed_date'].apply(lambda x : x.replace(day=1))
        d0=self.current_date
        # calculate the total days till now of the video
        df_3['days']=d0-df_3['pb_date']
        
        d1=timedelta(days=90)

        
        # round value to 2 decimal places
        df_3['percentage']=round(df_3['percentage'],2)
        # split video older than 3 months
        df_4=df_3.loc[(df_3['days']>d1)]
        #function for calculating number of months 
        # get number of months between published date and current date
        def month_diff(d1):
            return (self.current_date.year-d1.year) * 12 + self.current_date.month-d1.month +1

        # get total number of month from published date
        df_4['month']=df_4['publilshed_date'].apply(month_diff)
        # print month min
        print(df_4.month.min())
        # print df_4 columns
        print(df_4.columns)
        
        # function to split month wise percentage split for each video
        def make_list(month,per):
            # write logic to split month wise percentage for video
            total=100
            # for first 3 months range is between 60-70
            per_list=[]
            #for first month 
            per_list.append(random.uniform(31,37))
            # for second month 
            per_list.append(random.uniform(22,27))
            # for third month
            per_list.append(random.uniform(10,18))
            remaining=total-sum(per_list)-per
            print('remaining_value ',remaining)
            max_per_month=remaining/(month-3)
            while len(per_list)!=month-1:
                per_list.append(random.uniform(max_per_month-0.2,max_per_month))
            per_list.append(per)
    
            # check for total split percentage
            remaining_1=total-sum(per_list)
            # if there some remaining percentage left than adjust that in 2nd  month and 3rd month
            add_per=remaining_1/2
            # add that in 2nd month 
            per_list[1]=per_list[1]+add_per
            # add that in 3rd month 
            per_list[2]=per_list[2]+add_per
    
    
            return per_list
        
        # another function to split month wise percentage for each video
        def make_list_1(month,per):
            # write logic to split month wise percentage for video
            total=100
            # for first 3 months range is between 60-70
            per_list=[]
            #for first month 
            per_list.append(random.uniform(23,28))
            # for second month 
            per_list.append(random.uniform(34,38))
            # for third month
            per_list.append(random.uniform(10,18))
            remaining=total-sum(per_list)-per
            print('remaining_value ',remaining)
            max_per_month=remaining/(month-3)
            while len(per_list)!=month-1:
                per_list.append(random.uniform(max_per_month-0.2,max_per_month))
            per_list.append(per)
    
            # check for total split percentage
            remaining_1=total-sum(per_list)
            # if there some remaining percentage left than adjust that in 2nd  month and 3rd month
            add_per=remaining_1/2
            # add that in 2nd month 
            per_list[1]=per_list[1]+add_per
            # add that in 3rd month 
            per_list[2]=per_list[2]+add_per
    
    
            return per_list
        
        #get percentage of each video
        for x in df_4.index:
            cum_organic=0
            if df_4.loc[x,'publilshed_date'].day<15:
                per_list=make_list(df_4.loc[x,'month'],df_4.loc[x,'percentage'])
            else :
                per_list=make_list_1(df_4.loc[x,'month'],df_4.loc[x,'percentage'])
            print(" video month  ",df_4.loc[x,'month'])
            print(" video percentage  ",df_4.loc[x,'percentage'])

            print(per_list)
            print(" length of the list",len(per_list))
            print(" total sum of per list",sum(per_list))

            # now write logic for multiply views with each month starting from published date 
            pb_date=df_4.loc[x,'pb_date']
            print(df_4.loc[x,'pb_date'])
            print(df_4.loc[x,'Organic_views'])

        # now store that views in video published date month
        #     df_4.loc[x,str(pb_date)]=int(df_4.loc[x,'Organic_views']*per_list[0])/100
            index=0
            for p in range(len(per_list)-1):

                print("present month",pb_date,p,index)
                df_4.loc[x,str(pb_date)]=int((df_4.loc[x,'Organic_views']*per_list[p])/100)
                # get cumulatie Organic views
                cum_organic+=df_4.loc[x,str(pb_date)]
                # get next month date
                pb_date=self.next_month(pb_date)
                index+=1


        #     # adjust views in last month
            df_4.loc[x,str(self.current_date)]=df_4.loc[x,'Organic_views']-cum_organic
            

            print(df_4.loc[x])
            print("-----------------------------------------------------------")

        print(df_4.columns) 
        # Now make file for recent 4 months videos
        df_5=df_3.loc[~(df_3['days']>d1)]
        df_5['month']=df_5['publilshed_date'].apply(month_diff)
        # per split per mnth wise for videos , published date less than or equal to 3 months
        def make_list_3(month,per,day):
            # write logic to split month wise percentage for video
            total=100
            # for first 3 months range is between 60-70
            per_list=[]
            #remaning Per left to split for months
            remaining=total-per
            if month==1:
                per_list.append(total)
            elif month==2:
                per_list.extend([remaining,per])
            elif month==3:
                # 60 % per of remaining for first month
                if day>=1 and day<10:
                    print("here days is between 1 to 10")
                    first_month=remaining*.70
                    # 45% per of remaning for first month
                elif day>=10 and day<=20:
                    print("here days is between 10 to 20")
                    first_month=remaining*.55
                    # 25 % of remaining for first month
                else :
                    print("here days is between 20 to 30")
                    first_month=remaining*.35
                    
                per_list.append(first_month)
                remaining-=first_month
                per_list.append(remaining)
                per_list.append(per)
            else:
                print(" there might be some error check ")
            return per_list
        # START FROM HERE
        #get percentage of each video
        for x in df_5.index:
            per_list=make_list_3(df_5.loc[x,'month'],df_5.loc[x,'percentage'],df_5.loc[x,'publilshed_date'].day)
            cum_organic=0
            print("video published date",df_5.loc[x,'publilshed_date'])
            print(" video month  ",df_5.loc[x,'month'])
            print(" video percentage  ",df_5.loc[x,'percentage'])

            print(per_list)
            print(" length of the list",len(per_list))
            print(" total sum of per list",sum(per_list))

            # now write logic for multiply views with each month starting from published date 
            pb_date=df_5.loc[x,'pb_date']
            print(df_5.loc[x,'pb_date'])
            print(df_5.loc[x,'Organic_views'])
            #     print("-----------------------------------------------------------")
            # now store that views in video published date month
            #     df_4.loc[x,str(pb_date)]=int(df_4.loc[x,'Organic_views']*per_list[0])/100
            index=0
            for p in range(len(per_list)-1):

                print("present month",pb_date,p,index)
                df_5.loc[x,str(pb_date)]=int((df_5.loc[x,'Organic_views']*per_list[p])/100)
                # get cumulatie Organic views
                cum_organic+=df_5.loc[x,str(pb_date)]
                # get next month date
                pb_date=self.next_month(pb_date)
                index+=1


            # adjust views in last month
            df_5.loc[x,str(self.current_date)]=df_5.loc[x,'Organic_views']-cum_organic


            print(df_5.loc[x])
            print("-----------------------------------------------------------")
        print("columns for df_5",df_5.columns)
        #concatenate old and new videos data
        df_6=pd.concat([df_4,df_5],axis=0)
        #dropping not required columns
        df_6.drop(['days','month','new_created_date','pb_date','percentage','video_id_list'],axis=1,inplace=True)
        #convert columns to list 
        col=df_6.columns.tolist()
        print("df_6 columns ",col)
        columns_1=['videoid','channel_id','views','likes','dislikes','comment','publilshed_date','AD_views','Organic_views']
        col=[x for x in col if x not in columns_1]
        col=columns_1+col
        print("new columns for df_7",col)
        df_7=df_6[col]
        return df_7

        
    

# make calss object and pass current month date
        
obj=New_link_MonthlySplit(date(2019,6,1))
# read file for videos id 
df=pd.read_csv('video_id_task.csv')
# Make monthly split for AD views 
df_2=obj.AD_Monthly_split(df)
df_2.columns
# read daily tracking file 
df_1=pd.read_csv('video_log_task.csv')
# pre process daily tracking file for Organic split
df_1=obj.daily_tracking(df_1)
df_1=df_1.reset_index()
# Calling function for Organic split mmonthly
# note : df_1 : daily tracking file
# df_2 is AD split monthky file
organic_df=obj.Organic_monthly_split(df_1,df_2)
# make file for AD split and organic split monthly split
path='saregama_monthly_split_new_links.xlsx'
writer=pd.ExcelWriter(path,engine='xlsxwriter')
df_2.to_excel(writer,sheet_name='Ad')
organic_df.to_excel(writer,sheet_name='Organic')
writer.save()
writer.close()

        
        
        
