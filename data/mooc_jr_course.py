# -*- coding: UTF-8 -*-
from selenium import webdriver
from pyquery import PyQuery as pq
import time
import redis
import chardet
import requests
from itertools import chain
import pymysql

from selenium.webdriver.chrome.options import Options
# r = redis.from_url('redis://@localhost:6379/1')
# pool = redis.ConnectionPool(host='localhost', port=6379,decode_responses=True)
# r = redis.Redis(connection_pool=pool)
# r.flushall()

#连接MYSQL数据库
db = pymysql.connect(host='127.0.0.1',user='root',password='ASElab',db='courseinfo',port=3306,charset='utf8')
#db = MySQLdb.connect('127.0.0.1','root','mysql','test',coon.set_character_set('utf8'))
print('连接数据库成功！')
conn = db.cursor() #获取指针以操作数据库
conn.execute('set names utf8')

# pageSourceUrl = "https://www.icourse163.org/search.htm?search=%E9%87%91%E8%9E%8D#/"


# chrome = webdriver.PhantomJS(executable_path=r'F:\PycharmProjects\phantomjs-2.1.1-windows\bin\phantomjs.exe')

# print (driver.page_source)

sql_insert_course_info = '''INSERT INTO course_info_2(course_id, course_name, course_label, course_affiliation, course_teacher, course_overview, course_presession)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
sql_clean_course_info = 'truncate table course_info_2;'

def getSinglePageUrl(pageSourceUrl):
    code = pq(pageSourceUrl)
    href = code("#j-courseCardListBox a")
    # for i in href:
    #     print(str(code(i).attr("href")))
    urlList = []
    for i in href:
        temp = "http:" + str(code(i).attr("href"))
        if temp.__contains__("www") and not temp.__contains__("https"):
            print (temp)
            urlList.append(temp)
    urlList = list(set(urlList))
    return urlList

def getAllPageUrl(pageSourceUrl):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(executable_path='F:\PycharmProjects\chromedriver\chromedriver.exe',
                              chrome_options=chrome_options)
    driver.get(pageSourceUrl)

    allUrl = []
    count = 1
    while (count < 3):
        allUrl = chain(allUrl, getSinglePageUrl(driver.page_source))
        print ("第%d页"%count)
        # print (driver.find_element_by_class_name("ux-pager_btn__next").get_attribute("class"))
        driver.find_element_by_link_text("下一页").click()
        time.sleep(0.1)
        if(driver.find_element_by_link_text("下一页").get_attribute("class") == "th-bk-disable-gh"):
            allUrl = chain(allUrl, getSinglePageUrl(driver.page_source))
            break
        count += 1
    driver.quit()
    return allUrl

def saveCourseInfoes(courseUrlList = []):
    count, index = 0, 0
    errorList = []
    # conn.execute(sql_clean_course_info)
    while(count < courseUrlList.__len__()):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(executable_path='F:\PycharmProjects\chromedriver\chromedriver.exe',
                                  chrome_options=chrome_options)
        driver.get(courseUrlList[count])
        html = driver.page_source
        info = pq(html)
        t = info(".f-richEditorText")
        print (info(".course-title.f-ib.f-vam").text())
        # print (info(t[0]).text())
        # print (info(t[1]).text())
        # print (info(t[2]).text())
        if(len(t) >= 3):
            # r.hset("CourseInfo", index, {
            #     "courseName": info(".course-title.f-ib.f-vam").text().encode(encoding="unicode_escape"),  #课程名称
            #     "courseLabel": info(".breadcrumb_item.sub-category").text().encode(encoding="UTf-8"),  #课程标签
            #     "affiliationInfo": info(".u-img").attr('alt').encode(encoding="UTf-8"),  #课程机构
            #     "teacherInfo": info(".cnt.f-fl").children('.f-fc3').text().replace("\n", " ").encode(encoding="UTf-8"),  #授课教师
            #     "anOverviewOfTheCourse": info(t[0]).text().encode(encoding="UTf-8"),  #课程简介
            #     "teachingObjectives": info(t[2]).text().encode(encoding="UTf-8"),  #先修课程unicode_escape"),  # 课程名称
            courseName = info(".course-title.f-ib.f-vam").text(),
            courseLabel = info(".breadcrumb_item.sub-category").text(),  # 课程标签
            courseAffiliation = info(".u-img").attr('alt'),  # 课程机构
            courseTeacher = info(".cnt.f-fl").children('.f-fc3').text().replace("\n", " "),  # 授课教师
            courseOverview = info(t[0]).text(),  # 课程简介
            coursePresession = info(t[2]).text(),  # 先修课程

            index += 1

            t = [index, courseName, courseLabel, courseAffiliation, courseTeacher, courseOverview, coursePresession]
            conn.execute(sql_insert_course_info, t)
            db.commit()
            # print ("插入数据成功！")
        else:
            errorList.append(courseUrlList[count])
        count += 1
    return errorList

timeStart = time.time()
allUrl = getAllPageUrl("https://www.icourse163.org/search.htm?search=%E9%87%91%E8%9E%8D#/")
errorList = saveCourseInfoes(list(allUrl))
print("\n\n")
for i in errorList:
    print(i)
# print("共耗时:", end = " ")
# print(time.time() - timeStart)
conn.close()
db.close()