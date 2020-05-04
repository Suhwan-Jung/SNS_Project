#!/usr/bin/env python
# coding: utf-8

# In[24]:


# module import 

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

import time
import requests
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pymysql 
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
import MySQLdb
from multiprocessing import Pool

import pickle

from konlpy.tag import Okt
from konlpy.tag import Kkma
from konlpy.utils import pprint

import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
from collections import Counter
from wordcloud import WordCloud
import math
import numpy as np


# In[25]:


class youtube_analysis:
    
    """
    1. Youtube main search contents information crawling
    2. In Youtube main, each video's information crawling, ex) running time, comment, etc..
    3. Distinguish Each video's language 
    4. Make a Wordcloud
    """
    
    def __init__(self):
        print("Youtube analysis")
    
    
    ##  ----Youtube Crawling function line----
    
    def search_main(self,search_object,chrome_driver_root,filter = None):
        
        # input information
        # search_object = object that you want to searching in youtube
        # chrome_driver_root = your computer's root that exists chrome driver
        
        # time_filter = filter how latest you want to crawl youtube data,
        ### default value none_filter
        ### 'hour','year','week','month','day'
        
        chrome_driver = chrome_driver_root
        driver = webdriver.Chrome(chrome_driver)

        ### time filter, this is empirical, you may need to edit later ### 
        oneday = "&sp=EgQIAhAB"
        oneweek = "&sp=EgQIAxAB"
        onemonth = "&sp=EgQIBBAB"
        oneyear = "&sp=EgQIBRAB"
        onehour = "&sp=EgQIARAB"
        none_filter = ''
        
        if filter == 'hour' :
            time_filter = onehour
        elif filter == 'year':
            time_filter = oneyear
        elif filter == 'week' :
            time_filter = oneweek
        elif filter == 'month' :
            time_filter = onemonth
        elif filter == 'day':
            time_filter = oneday
        else :
            time_filter = none_filter
        
        # url that you want to search that object and start driver
        search_input = search_object
        url = 'https://www.youtube.com/results?search_query={}{}'.format(search_object, time_filter)
        driver.get(url)

        last_page_height = driver.execute_script("return document.documentElement.scrollHeight")

        # selenium auto-scroll function
        # if youtube page is ended, than chrome driver page is down
        while True:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            
            time.sleep(2.0) # sleep for computer's crawling loading 
            
            new_page_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_page_height == last_page_height:
                break
            last_page_height = new_page_height


        src = driver.page_source
        soup = BeautifulSoup(src)
        driver.close()

        data = soup.select('#video-title')

        print('the number : ',len(data),' title loaded')

        date_list = []
        title_list = []
        uploader_list = []
        views_list = []
        running_time_list = []
        link_list = []
        
        
        # saving and preprocessing information 
        for i in range(len(data)):
           
            uploaded_time = datetime.datetime.now()
            
            if 'class="yt-simple-endpoint style-scope ytd-video-renderer"' in str(data[i]):
                pass
            else :
                continue
            contents = str(data[i]).split('class="yt-simple-endpoint style-scope ytd-video-renderer"')[0].strip('<a aria-label=')
            
            need_to_strip = contents[0]
            contents = contents.strip(need_to_strip)
            
            link = str(data[i]).split('class="yt-simple-endpoint style-scope ytd-video-renderer"')[1].split('id="video-title"')[0].strip(' href="')
            views = contents.split(' 조회수 ')[-1].split('회')[0]
            
            if contents.split(' 조회수 ')[-2].split(' 전')[-1] == '':
                running_time = ''
            else : 
                running_time = contents.split(' 조회수 ')[-2].split(' 전 ')[-1]

            upload_time = contents.split(' 조회수 ')[-2].split(' 전 ')[-2].split(' ')[-1]

            
            # time preprocessing
            if '초' in upload_time :
                delta = int(upload_time.strip('초'))
                uploaded_time -= datetime.timedelta(seconds=delta)
                uploaded_time = uploaded_time.date()
            elif '분' in upload_time :
                delta = int(upload_time.strip('분'))
                uploaded_time -= datetime.timedelta(minutes = delta)
                uploaded_time = uploaded_time.date()
            elif '시간' in upload_time :
                delta = int(upload_time.strip('시간'))
                uploaded_time -= datetime.timedelta(hours = delta)
                uploaded_time = uploaded_time.date()
            elif '일' in upload_time :
                delta = int(upload_time.strip('일'))
                uploaded_time -= datetime.timedelta(days = delta)
                uploaded_time = uploaded_time.date()
            elif '주' in upload_time:
                delta = int(upload_time.strip('주'))
                uploaded_time -= datetime.timedelta(weeks = delta)
                uploaded_time = uploaded_time.date()
            elif '개월' in upload_time:
                delta = int(upload_time.strip('개월'))
                uploaded_time -= relativedelta(months=delta)
                uploaded_time = uploaded_time.date()
            elif '년' in upload_time :
                delta = int(upload_time.strip('년'))
                uploaded_time -= relativedelta(years=3)
                uploaded_time = uploaded_time.date()
            else :
                pass 

            if running_time == '' :
                uploader = contents.split(' 조회수 ')[-2].split(' 전')[-2].split(' 게시자: ')[-1].strip(upload_time).strip()
                contents = contents.split(' 조회수 ')[-2].split(' 전')[-2].split(' 게시자: ')[0]
            else :
                uploader = contents.split(' 조회수 ')[-2].split(' 전 ')[-2].split(' 게시자: ')[-1].strip(upload_time).strip()
                contents = contents.split(' 조회수 ')[-2].split(' 전 ')[-2].split(' 게시자: ')[0]
            
            date_list.append(uploaded_time)
            title_list.append(contents)
            uploader_list.append(uploader)
            views_list.append(views)
            running_time_list.append(running_time)
            link_list.append(link)
            

        df_search_youtube = pd.DataFrame({"date" : date_list,"title" : title_list,"uploader" : uploader_list,"views" : views_list,"running_time" : running_time_list,'url': link_list})
        df_search_youtube.views = df_search_youtube.views.str.replace('없음', '0')
        df_search_youtube.views = df_search_youtube.views.str.replace(',', '').astype('int64')
        df_search_youtube.drop_duplicates(["url"],inplace = True)

        # output information
        # type : pd.dataframe
        # column : uploaded_date, video_title, uploader, views, video_running_time, url
        
        return df_search_youtube
    
    def search_comment(self,url,chrome_driver_root):
        
        
        chrome_driver = chrome_driver_root
        driver = webdriver.Chrome(chrome_driver)
        link = "https://www.youtube.com"+url
        driver.get(link)

        last_page_height = driver.execute_script("return document.documentElement.scrollHeight")

        # selenium auto-scroll function
        # if youtube page is ended, than chrome driver page is down
        while True:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            
            time.sleep(2.0) # sleep for computer's crawling loading 
            
            new_page_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_page_height == last_page_height:
                break
            last_page_height = new_page_height

        src = driver.page_source
        soup = BeautifulSoup(src,'lxml')
        driver.close()
        
        user_ID = soup.select('div#header-author > a > span')
        comment_time = soup.select("#header-author > yt-formatted-string > a")
        comment_text = soup.select("#content-text")
        comment_like = soup.select("span#vote-count-left")

        userID_list = [] 
        comments_list = []
        comment_time_list = []
        like_list = []
        url_list = []

        for i in range(len(user_ID)): 
            userID_tmp = str(user_ID[i].text) 
            # print(str_tmp) 
            userID_tmp = userID_tmp.replace('\n', '') 
            userID_tmp = userID_tmp.replace('\t', '') 
            userID_tmp = userID_tmp.replace(' ','') 
            userID_list.append(userID_tmp) 

            comment_time_tmp = str(comment_time[i].text) 
            # print(str_tmp) 
            comment_time_tmp = comment_time_tmp.replace('\n', '') 
            comment_time_tmp = comment_time_tmp.replace('\t', '') 
            comment_time_tmp = comment_time_tmp.replace(' 전','')
            comment_time_tmp = comment_time_tmp.replace('(수정됨)','') 
            commented_time = datetime.datetime.now()

            if '초' in comment_time_tmp :
                delta = int(comment_time_tmp.strip('초'))
                commented_time -= datetime.timedelta(seconds=delta)
                commented_time = commented_time.date()
            elif '분' in comment_time_tmp :
                delta = int(comment_time_tmp.strip('분'))
                commented_time -= datetime.timedelta(minutes = delta)
                commented_time = commented_time.date()
            elif '시간' in comment_time_tmp :
                delta = int(comment_time_tmp.strip('시간'))
                commented_time -= datetime.timedelta(hours = delta)
                commented_time = commented_time.date()
            elif '일' in comment_time_tmp :
                delta = int(comment_time_tmp.strip('일'))
                commented_time -= datetime.timedelta(days = delta)
                commented_time = commented_time.date()
            elif '주' in comment_time_tmp:
                delta = int(comment_time_tmp.strip('주'))
                commented_time -= datetime.timedelta(weeks = delta)
                commented_time = commented_time.date()
            elif '개월' in comment_time_tmp:
                delta = int(comment_time_tmp.strip('개월'))
                commented_time -= relativedelta(months=delta)
                commented_time = commented_time.date()
            elif '년' in comment_time_tmp:
                delta = int(comment_time_tmp.strip('년'))
                commented_time -= relativedelta(years=3)

            else :
                pass
            comment_time_list.append(commented_time) 

            comments_tmp = str(comment_text[i].text) 
            comments_tmp = comments_tmp.replace('\n', ' ') 
            comments_tmp = comments_tmp.replace('\t', ' ') 
            comments_tmp = comments_tmp.strip(' ') 
            comments_list.append(comments_tmp)

            like_tmp = str(comment_like[i].text) 
            # print(str_tmp) 
            like_tmp = like_tmp.replace('\n', '') 
            like_tmp = like_tmp.replace('\t', '') 
            like_tmp = like_tmp.replace(' ','')

            if '천' in like_tmp :
                like_tmp = like_tmp.split('천')[0]
                like_tmp = float(like_tmp) * 1000

            like_tmp = int(like_tmp)
            like_list.append(like_tmp)  

            url_list.append(url)

        df_comment_youtube = pd.DataFrame({"date" : comment_time_list,"userID" : userID_list,"comment" : comments_list,"likes" : like_list,'url': url_list})
        df_comment_youtube["date"] = df_comment_youtube["date"].astype(str)
        
        print(len(userID_list),'comments loaded')
        
        return df_comment_youtube
        
    ##  ----Youtube Mysql function line----
    
    
    def Mysql_df_save(self,df_save,id_,pw_,host,DB,table_name):
        
        engine = create_engine("mysql+mysqldb://{}:".format(id_)+"{}".format(pw_)+"@{}/{}".format(host,DB), encoding='utf-8')
        conn = engine.connect()
        df_save.to_sql(name='{}'.format(table_name), con=engine, if_exists='append',index=False)
        
        print("=====Saving dataframe to Mysql is completed=====")

    def Mysql_df_load(self,id_,pw_,host,port,DB,table_name):
        
        db = pymysql.connect(host = '{}'.format(host), port = port,user ='{}'.format(id_),passwd = '{}'.format(pw_),db='{}'.format(DB),charset = 'utf8')
        cursor = db.cursor()
        sql = """ select * from {} ;
              """.format(table_name)
        cursor.execute(sql)
        data = pd.DataFrame(cursor.fetchall())
        
        return data

    
    ##  ----Youtube language function line----
    ## if you want to practice only korean analysis, than you don't need this functions
    
    def isHangul(self, text):
        hanCount = len(re.findall(u'[\u3130-\u318F\uAC00-\uD7A3]+', text))
        if hanCount > 0 :
            return True
        else :
            return False
        
    def isEnglish(self,text):
    
    
        text_list = text.split()
        wrong_count = 0
        correct_count = 0
    
        for idx in text_list :

            if 'en' == langid.classify(idx)[0] :
                correct_count += 1
            else :
                wrong_count+= 1
        
        # when there is English name in Search title(for translation) 
        if correct_count > wrong_count + 1 :
            return True
        else :
            return False
        
    def classifying_language(self, dataframe):
    
        # output list
        Kor_url_list = []
        Eng_url_list = []
        Etc_url_list = []

        for index in dataframe.index:
            if isHangul(dataframe.loc[index].title):
                Kor_url_list.append(dataframe.loc[index].url)
            elif isEnglish(dataframe.loc[index].title):
                Eng_url_list.append(dataframe.loc[index].url)
            else : 
                text_list = dataframe.loc[index].title.split()
                
                # Sometimes, distribution error happened
                # this err is empirical detection
                wrong_count = 0
                correct_count = 0

                for idx in text_list :
                    if 'ko' == langid.classify(idx)[0] :
                        correct_count += 1
                    else :
                        wrong_count+= 1

                if correct_count > wrong_count :
                    Kor_url_list.append(dataframe.loc[index].url)

                else :     
                    Etc_url_list.append(dataframe.loc[index].url)

        return Kor_url_list, Eng_url_list, Etc_url_list
    
    ##  ----restrict timeline----
    
    def select_date(self,df_input,str_start,str_end):
    
        # type str date input change to type datetime
        dt_start = datetime.datetime.strptime(str_start,"%Y-%m-%d").date()
        dt_end = datetime.datetime.strptime(str_end,"%Y-%m-%d").date()
        start_to_df_input = df_input[df_input["date"] >= dt_start]
        start_to_end_df_input = start_to_df_input[start_to_df_input["date"] <= dt_end]

        return start_to_end_df_input
    
    ##  ----distribute morpheme----
    
    def tokenizer_lang_class(self, df):
    
        print("-------------- tokenizing language start ----------------")    
        okt = Okt()
        res_token_df = pd.DataFrame(columns = ['date','token','likes','type'])
        df.reset_index(drop = True,inplace=True)
        k = 0     

        for idx in df.index :
            # step 출력
            if idx % 100 == 0 :
                print("step = ",idx, df.loc[idx])

            # tokenize
            token_list = okt.pos(df.loc[idx].comment) 

            # 새로운 데이터 프레임에 적용
            for token in token_list :    
                if token[-1] in ['Noun','Verb','Adjective'] :
                    res_token_df.loc[k] = [df.loc[idx].date,token[0],df.loc[idx].likes,token[-1]]
                    k += 1

        N_token = res_token_df[res_token_df["type"] == 'Noun']
        V_token = res_token_df[res_token_df["type"] == 'Verb']
        A_token = res_token_df[res_token_df["type"] == 'Adjective']         

        return N_token, V_token, A_token
    
    def morpheme_distribution(self,df):

        print("-------------- morpheme_distribution start ----------------")
        kkma = Kkma()
        NN_token_df = pd.DataFrame(columns = ['date','token','likes','type'])
        VV_token_df = pd.DataFrame(columns = ['date','token','likes','type'])
        VA_token_df = pd.DataFrame(columns = ['date','token','likes','type'])
        etc_token_df = pd.DataFrame(columns = ['date','token','likes','type'])

        df.reset_index(drop = True,inplace=True)
        k = 0     

        for idx in df.index :
            # step 출력
            if idx % 100 == 0 :
                print("step = ",idx, df.loc[idx])

            # tokenize
            token_list = kkma.pos(df.loc[idx].comment) 

            temp_token = ''
            for token in token_list :
                if 'N' in token[-1]: 
                    temp_token += token[0]
            for token in token_list :
                if 'VV' == token[-1]:
                    VV_token_df.loc[k] = [df.loc[idx].date,token[0],df.loc[idx].likes,token[-1]]
                elif 'VA' == token[-1]: 
                    VA_token_df.loc[k] = [df.loc[idx].date,token[0],df.loc[idx].likes,token[-1]]
                elif 'NN' in token[-1]: 
                    NN_token_df.loc[k] = [df.loc[idx].date,token[0],df.loc[idx].likes,token[-1]]
                else :
                    etc_token_df.loc[k] = [df.loc[idx].date,token[0],df.loc[idx].likes,token[-1]]
            k += 1
        NN_token_df.reset_index(drop = True,inplace=True)
        VV_token_df.reset_index(drop = True,inplace=True)
        VA_token_df.reset_index(drop = True,inplace=True)
        etc_token_df.reset_index(drop = True,inplace=True)

        return NN_token_df, VV_token_df, VA_token_df, etc_token_df
    
    def save_pickle(self, df,name):
        df.to_pickle("{}".format(name))
        print("{} save is completed".format(name))
    
    ##  ----making word cloud----
    
    def like_base_list(self,df,rank):

        like_multiple_list = []

        for idx in df.index:
        
            for count in range(df.loc[idx].likes):
                like_multiple_list.append(df.loc[idx].token)
        
        return like_multiple_list
    
    def make_wordcloud(self,list_,count,type_,save_name):

        counts = Counter(list_) 

        words = dict(counts.most_common(count))

        font_path = 'c:\\windows\\fonts\\NanumGothic.ttf'
        wc = WordCloud(font_path= font_path ,
                       background_color="white",
                       colormap = "Accent_r",
                       width = 1500,
                       height = 1000
                       ).generate_from_frequencies(words)

        plt.imshow(wc)
        plt.axis('off')
        plt.show()
        plt.imshow(wc)
        plt.savefig('wordcloud_{}'.format(save_name),dpi = 300)


# In[3]:



### crawling

if __name__ == '__main__':
    start_time = time.time()
    driver_root = 'C:/Users/user/Desktop/머신러닝과 데이터분석/chromedriver_win32/chromedriver.exe'
    
    
    analysis = youtube_analysis()
    
    # no1 - 필라이트 이미지 감정분석
    main_data = analysis.search_main(search_object =  '''"편의점 맥주"'''
                                     ,chrome_driver_root = driver_root
                                     ,filter = 'year')
    
    comment_df = pd.DataFrame(columns = ["comment_date" ,"userID" ,"likes" ,'url'])
    cnt = 1
    try :
        for url in main_data['url']:
            comment_df = pd.concat([comment_df,analysis.search_comment(url,driver_root)])
            
            if cnt % 50 == 0:
                print("count :",cnt)
            cnt+=1
    except Exception :
        if cnt % 50 == 0:
                print("count :",cnt)
        print(url,"Exception occurs")
        cnt += 1
    
    print("---- %s seconds ----" % (time.time() - start_time))
    
    analysis.Mysql_df_save(main_data,'root','sogangsp','localhost','sns_db','youtube_search_convenience')
    analysis.Mysql_df_save(comment_df,'root','sogangsp','localhost','sns_db','youtube_comment_convenience')

