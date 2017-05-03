# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re
from bs4 import BeautifulSoup
import sys
import time
import random
import json
import requests
import pandas as pd
from concurrent import futures
import datetime
import urllib2

PRINT = False
# PRINT = True

NUM_THREADS = 5

HOMEPAGE_URL = "http://bbs.uestc.edu.cn/"

LOGIN_PAGE_URL = "http://bbs.uestc.edu.cn/member.php?mod=logging&action=login&loginsubmit=yes&inajax=1"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36"}

def utoutf(tags):
    a = len(tags)
    for i in range(a):
        print unicode(str(tags[i]),'utf-8')

def print_jianduan():
    print "\n\n*************************************************************************************************"
# 爬取论坛首页-不登录
r_0 = requests.get(HOMEPAGE_URL,headers=HEADERS)
bs_obj = BeautifulSoup(r_0.text,"lxml")
a = bs_obj.find_all("a")[:10]

# 登陆
USERNAME="dianzixiaoxiao"
PASSWORD="19940501cky"

params={
    "username":USERNAME,
    "password":PASSWORD,
    "cookietime":2592000
}
r_login = requests.post(LOGIN_PAGE_URL,headers=HEADERS,params=params)


# 再次登陆就只需要上次登陆的cookies就可以了
r_1 = requests.get(HOMEPAGE_URL,headers=HEADERS,cookies=r_login.cookies)
bs_obj=BeautifulSoup(r_1.text,"lxml")




# 得到相应URl页面的内容
def get_bs_obj_from_url(http_url, cookies=""):# cookies设置了默认参数
    global HEADERS

    bs_obj = None
    exception_time = 0
    while True:
        try:
            if PRINT:
                print("Getting {}".format(http_url)) #格式化{}代表的位置
            r = requests.get(http_url,headers=HEADERS,cookies=cookies)
            bs_obj = BeautifulSoup(r.text,"lxml")
            break
        except Exception as e:
            if PRINT:
                print e
            exception_time += 1
            time.sleep(10)
            if exception_time > 10:
                break
    return bs_obj

bs_obj = get_bs_obj_from_url(HOMEPAGE_URL,cookies=r_login.cookies)





# 根据自增UID爬取用户信息
def get_person_info_from_uid(uid,cookies):
    http_url = "http://bbs.uestc.edu.cn/home.php?mod=space&uid={}&do=profile".format(uid)
    bs_obj = get_bs_obj_from_url(http_url,cookies=cookies)


    if bs_obj.find("div",class_="alert_error") is not None or bs_obj.text.find("等待验证会员") != -1 or bs_obj.text.find("在线时间") == -1:
        print "no exist"
        return None

    df = pd.DataFrame()
    stage = 0

    try:
        username = bs_obj.find("h2", class_="xs2").find("a").get_text()
        stage = 1

        o_stats_infos1 = bs_obj.find("ul",class_="cl bbda pbm mbm").find("li").find_all("a")
        num_friends = int(o_stats_infos1[0].get_text().split(" ")[1])
        num_reply = int(o_stats_infos1[3].get_text().split(" ")[1])
        num_threads = int(o_stats_infos1[4].get_text().split(" ")[1])
        stage = 2


        o_stats_infos = bs_obj.find("ul",{"id":"pbbs"}).find_all("li")
        for o_stats in o_stats_infos:
            o_stats.em.extract()
        online_time = int(o_stats_infos[0].get_text().split(" ")[0])
        register_time = o_stats_infos[1].get_text()
        stage = 3

        o_stats_infos2 = bs_obj.find("div",{"id":"psts"}).find("ul",class_="pf_l").find_all("li")
        for o_info in o_stats_infos2:
            o_info.em.extract()
        points = int(o_stats_infos2[1].get_text())
        weiwang = int((o_stats_infos2[2]).get_text().split(" ")[0])
        water_drops = int(o_stats_infos2[3].get_text().split(" ")[0])

        dfs = pd.DataFrame(data=[[uid, username, num_friends, num_reply, num_threads,
                                 online_time, register_time, points, weiwang, water_drops]],
                          columns=['ID', '用户名', '好友数', '回帖数', '主题数',
                                  '在线时间', '注册时间', '积分', '威望', '水滴'])

        df = df.append(dfs)
    except Exception as e:
        print(e,"| uid = {}, stage = {}".format(uid,stage))
    return df


def get_all_person_info(uid_start,uid_end,cookies):
    df_all = pd.DataFrame()
    count = 0
    with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_list=[]
        for uid in range(uid_start,uid_end+1):
            future_list.append(executor.submit(get_person_info_from_uid,uid,cookies))
        for future in futures.as_completed(future_list):
            if future.exception() is None:
                df_one = future.result()
                if df_one is not None:
                    df_all = df_all.append(df_one)
            count += 1
            sys.stdout.write("\rProgress:{}/{}".format(count,uid_end-uid_start+1))
    return df_all



def get_posts_from_uid_and_page(uid,page_no,cookies):
    http_url = "http://bbs.uestc.edu.cn/home.php?mod=space&uid={}&do=thread&view=me&order=dateline&from=space&page={}"\
    .format(uid,page_no)
    bs_obj = get_bs_obj_from_url(http_url,cookies)

    df_posts = pd.DataFrame()
    try:
        o_posts = bs_obj.find("form", {"id": "delform"}).find_all("tr")

        for o_post in o_posts[1:]:
            title = o_post.find("th").find("a").get_text()
            tid = int(o_post.find("th").find("a").attrs['href'].split("=")[-1])
            o_tds = o_post.find_all("td")
            forum = o_tds[1].find('a').get_text()
            replys = int(o_tds[2].find('a').get_text())
            views = int(o_tds[2].find('em').get_text())

            df_post = pd.DataFrame(data=[[uid, title, tid, forum, replys, views]],columns=['UID', '标题', 'TID', '板块', '回复数', '查看数'])
            df_posts = df_posts.append(df_post)
    except Exception as e :
        pass

    return df_posts



def get_posts_from_uid(uid,num_posts,cookies):
    num_pages = int((num_posts+19)/20)

    df_uid_all_posts = pd.DataFrame()

    with futures.ThreadPoolExecutor(max_workers=(NUM_THREADS/2)) as executor:
        future_list = []
        for page_no in range(1,num_pages+1):
            future_list.append(executor.submit(get_posts_from_uid_and_page,uid,page_no,cookies))
        for future in futures.as_completed(future_list):
            if future.exception() is None:
                df_uid_posts_page = future.result()
                df_uid_all_posts = df_uid_all_posts.append(df_uid_posts_page)
    return df_uid_all_posts


def get_posts_from_person_infos(df_person_info,cookies):
    df_all_posts = pd.DataFrame()

    with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_list=[]
        count = 0
        for uid in df_person_info['ID'].values:
            future_list.append(executor.submit(get_posts_from_uid, uid, df_person_info[df_person_info['ID']==uid]['主题数'].ix[0], cookies=r_login.cookies))

            for future in futures.as_completed(future_list):
                if future.exception() is None:
                    df_uid_posts = future.result()
                    df_all_posts = df_all_posts.append(df_uid_posts)
                else:
                    print (future.exception())

                count += 1
                sys.stdout.write("\rProgress:{}/{}".format(count,len(df_person_info)))
    return df_all_posts




if __name__=="__main__":
    PART = 4
    MAX_UID = 10000
    df_person_info = pd.DataFrame()
    df_all_posts = pd.DataFrame()
    r_login = requests.post(LOGIN_PAGE_URL,headers=HEADERS,params=params)
    # for i in range(PART):
        # df_person_info = df_person_info.append(get_all_person_info(int(MAX_UID * i / PART), int(MAX_UID * (i+1) / PART),cookies=r_login.cookies))
        # print("\n{} Completed".format(int(MAX_UID * (i+1) / PART)))
    df_person_info = df_person_info.append(get_all_person_info(179097,179150,cookies=r_login.cookies))
    # df_person_info = df_person_info.append(get_all_person_info(10000,10005,cookies=r_login.cookies))

    writer = pd.ExcelWriter("qweqwe.xlsx")
    df_person_info.to_excel(writer, "Data")
    writer.save()
    print_jianduan()
    print df_person_info.sort_values(by='回帖数', ascending=False)[:10]
    print_jianduan()
    '''for i in range(PART):
    #     # df_person_info.append(get_all_person_info(int(MAX_UID*i/PART),int(MAX_UID*(i+1)/PART),cookies=r_login.cookies))
        df_person_info.append(get_all_person_info(int(MAX_UID * i / PART), int(MAX_UID * (i+1) / PART),cookies=r_login.cookies))
        print ("\n{} completed".format(int(MAX_UID*(i+1)/PART)))
    '''

    for i in range(PART):
        df_all_posts =  df_all_posts.append(get_posts_from_person_infos(df_person_info[int(len(df_person_info)*i/PART):int(len(df_person_info)*(i+1)/PART)],cookies = r_login.cookies))
        print ("\n{} Completed | {}".format(int(len(df_person_info)*(i+1)/PART),datetime.datetime.now()))
    writer_post = pd.ExcelWriter("post.xlsx")
    df_all_posts.to_excel(writer_post, "Data")
    writer_post.save()



