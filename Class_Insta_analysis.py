#!/usr/bin/env python
# coding: utf-8

# In[1]:


# filite 태그 검색후 게시물의 url을 얻어와서 각각의 크롤링을 진행함
from bs4 import BeautifulSoup
import selenium.webdriver as webdriver
import urllib.parse
from urllib.request import Request, urlopen
from time import sleep
import pandas as pd
import datetime


import pymysql 
from sqlalchemy import create_engine
import time
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


# In[2]:


class insta_analysis :
    
    def __init__(self):
        print("Insta analysis")
        
    ## ----Instagram Crawling function ine----
    
    def search_tag(self,tag,chrome_driver_root):
        
        
        chrome_driver = chrome_driver_root
        driver = webdriver.Chrome(chrome_driver)

        search = tag
        search = urllib.parse.quote(search)
        url = 'https://www.instagram.com/explore/tags/'+str(search)+'/'
        driver.get(url) 
        sleep(5)


        SCROLL_PAUSE_TIME = 1.0

        realtitle = []
        img_type = []
        upload_date = []
        reallink = []
        date_obj = None
        while True:
            pageString = driver.page_source
            bsObj = BeautifulSoup(pageString, "lxml")
            sleep(SCROLL_PAUSE_TIME)
            for link1 in bsObj.find_all(name="div",attrs={"class":"Nnq7C weEfm"}):
                
                for idx in range(len(link1)):
                    try:
                        title = link1.select('a')[idx] 
                        real = title.attrs['href']
                        reallink.append(real) 

                        temp_title = str(link1.select("img")[idx]).split('''class=''')[0].split("img alt=")[-1].strip().strip('''"''')
                        if "이미지: " in temp_title :
                            title_type = temp_title.split("이미지: ")[-1]

                            if " on " in temp_title :
                                date_str = temp_title.split("이미지: ")[-2].split(" on ")[-1].strip(". ")

                                if " tagging " in date_str:
                                    date_str = date_str.split(" tagging ")[0]
                                date_obj = datetime.datetime.strptime(date_str,'%B %d, %Y').date()
                            else :
                                pass # 앞 뒤 데이터의 날짜가 동일하다는 가정하 데이터 누락시 전 데이터 날짜 입력
                            real_title = temp_title.split("이미지: ")[-2].split(" on ")[0].strip("Photo by ")
                        else :
                            title_type = None
                            real_title = temp_title

                        img_type.append(title_type)
                        upload_date.append(date_obj)
                        realtitle.append(real_title)
                    except :
                        print("err happened")
              
                    
            last_height = driver.execute_script("return document.body.scrollHeight")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                sleep(SCROLL_PAUSE_TIME*5)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break

                else:
                    last_height = new_height
                    continue

        reallinknum = len(reallink)
        print(str(reallinknum)+"uploaded.")
        
        # In case of crawling instagram, the insta corp restrict crawling speed 
        # so your return value is not equal to searching result in Instagram by yourself 

        insta_search_df = pd.DataFrame({"date":upload_date, "title":realtitle, "img_type":img_type, "link":reallink})
        driver.close()
        
        return insta_search_df
    
    def hashtag(self,linklist):
        SCROLL_PAUSE_TIME = 1.0
        hash_tag_df = pd.DataFrame(columns = ['link','hashtag'])
        err_list = []
        for link in linklist:
            
            try:
                url = "https://www.instagram.com/p"+link

                req = Request(url,headers = {'User-Agent':'Mozilla/5.0'})
                src = urlopen(req).read()

                soup = BeautifulSoup(src,"lxml",from_encoding = 'utf-8')
                soup1 = soup.find("meta",attrs={"property":"og:description"})
                reallink1 = soup1['content']
                reallink1 = reallink1[reallink1.find("@")+1:reallink1.find(")")]
                reallink1 = reallink1[:]

                if reallink1 == '':
                    reallink1 = 'Null'

                for reallink2 in soup.find_all("meta",attrs={"property":"instapp:hashtags"}): 
                    reallink2 = reallink2['content']
                    hash_tag_df = hash_tag_df.append({"link":link,"hashtag":reallink2},ignore_index=True )
            except:
                err_list.append(link)
                sleep(SCROLL_PAUSE_TIME*5)
                print("err happened")
            print(link + 'completed')
        return hash_tag_df, err_list

    ##--------- for Mysql ----------
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
    
    
    ##---------- data analysis ------------
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
        # column name need to comment_date
        
        dt_start = datetime.datetime.strptime(str_start,"%Y-%m-%d").date()
        dt_end = datetime.datetime.strptime(str_end,"%Y-%m-%d").date()
        start_to_df_input = df_input[df_input["comment_date"] >= dt_start]
        start_to_end_df_input = start_to_df_input[start_to_df_input["comment_date"] <= dt_end]

        return start_to_end_df_input
    
    ##  ----distribute morpheme----
    
    def tokenizer_lang_class(self, df):
    
        print("-------------- tokenizing language start ----------------")    
        okt = Okt()
        res_token_df = pd.DataFrame(columns = ['token','type'])
        df.reset_index(drop = True,inplace=True)
        k = 0     

        for idx in df.index :
            # step 출력
            if idx % 100 == 0 :
                print("step = ",idx, df.loc[idx])

            # tokenize
            token_list = okt.pos(df.loc[idx].hashtag) 

            # 새로운 데이터 프레임에 적용
            for token in token_list :    
                if token[-1] in ['Noun','Verb','Adjective'] :
                    res_token_df.loc[k] = [token[0],token[-1]]
                    k += 1

        N_token = res_token_df[res_token_df["type"] == 'Noun']
        V_token = res_token_df[res_token_df["type"] == 'Verb']
        A_token = res_token_df[res_token_df["type"] == 'Adjective']         

        return N_token, V_token, A_token
    
    def morpheme_distribution(self,df):

        print("-------------- morpheme_distribution start ----------------")
        kkma = Kkma()
        NN_token_df = pd.DataFrame(columns = ['token','type'])
        VV_token_df = pd.DataFrame(columns = ['token','type'])
        VA_token_df = pd.DataFrame(columns = ['token','type'])
        etc_token_df = pd.DataFrame(columns = ['token','type'])

        df.reset_index(drop = True,inplace=True)
        k = 0     

        for idx in df.index :
            # step 출력
            if idx % 100 == 0 :
                print("step = ",idx, df.loc[idx])

            # tokenize
            token_list = kkma.pos(df.loc[idx].token) 

            temp_token = ''
            for token in token_list :
                if 'N' in token[-1]: 
                    temp_token += token[0]     
            if temp_token == df.loc[idx].token :
                NN_token_df.loc[k] = [df.loc[idx].token,token_list[0][-1]]
                k += 1
            else :
                for token in token_list :
                    if 'VV' == token[-1]:
                        VV_token_df.loc[k] = [token[0],token[-1]]
                    elif 'VA' == token[-1]: 
                        VA_token_df.loc[k] = [token[0],token[-1]]
                    elif 'NN' in token[-1]: 
                        NN_token_df.loc[k] = [token[0],token[-1]]
                    else :
                        etc_token_df.loc[k] = [token[0],token[-1]]
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

    
    def make_wordcloud(self,list_,count,save_name):

        counts = Counter(list_) 

        words = dict(counts.most_common(count))
        
        try :
            words.pop(save_name)
        except :
            pass
        try :
            words.pop('?')
        except :
            pass
        try :
            words.pop('맞팔')
        except :
            pass
        try :     
            words.pop('선팔')
        except :
            pass
        try :
            words.pop('선팔하면맞팔')
        except :
            pass
        try :
            words.pop('좋아요')
        except :
            pass
        try :
            words.pop('소통')
        except :
            pass 
        
        try :
            words.pop('좋반')
        except :
            pass
        
        try :
            words.pop('??')
        except :
            pass
    
            
            
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


if __name__ == '__main__':
    start_time = time.time()
    chrome_driver_root = 'C:/Users/user/Desktop/머신러닝과 데이터분석/chromedriver_win32/chromedriver.exe'
    insta = insta_analysis()
   
    
    start_time = time.time()

    df2 = insta.Mysql_df_load(id_ = 'root'
                        ,pw_ = 'sogangsp' 
                        ,host = 'localhost'
                        ,port = 3306 
                        ,DB = 'sns_db'
                        ,table_name = 'insta_tag_chungdo')
    df2.drop(0,axis=1,inplace = True)
    df2.rename({1:'date',2:'title',3:'img_type',4:'link'},axis=1,inplace = True)
    #####################
    start_time = time.time()
    
    hashtag_chungdo,err2 = insta.hashtag(df2.link)
    insta.Mysql_df_save(hashtag_chungdo
                  ,id_ = 'root'
                  ,pw_ = 'sogangsp'
                  ,host = 'localhost'
                  ,DB = 'sns_db'
                  ,table_name = 'hashtag_chungdo')
    
    print("---- %s seconds ----" % (time.time() - start_time))

