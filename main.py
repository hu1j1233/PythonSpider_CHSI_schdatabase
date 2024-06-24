# 导入必要的库
import requests
from lxml import etree
import csv
import undetected_chromedriver as uc
from undetected_chromedriver.options import ChromeOptions #由于学信网反爬虫设置原因，需要使用反反爬虫的浏览器模拟库
from undetected_chromedriver import By
import time
from requests.cookies import RequestsCookieJar
import pymysql
import json
from datetime import datetime
import os

"""
PythonSpider_CHSI_schdatabase
Author:Hu1j1233(aka.Huiji233)
Github:https://github.com/hu1j1233/PythonSpider_CHSI_schdatabase
For GZASC_2021_NEC1_GroupWork
"""
class MySQLHandler:
    """
    用于与MySQL数据库交互的处理类。
    方法:
    - connect: 建立数据库连接。
    - disconnect: 断开数据库连接。
    - create_table: 创建数据库表。
    - insert_data: 向数据库表中插入数据。
    """
    def __init__(self, host, user, password, database):
        """
        初始化数据库连接参数。
        参数:
        - host: 数据库主机地址。
        - user: 数据库用户名。
        - password: 数据库密码。
        - database: 数据库名称。
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        """
        建立与MySQL数据库的连接。
        """
        self.connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    def disconnect(self):
        """
        断开与MySQL数据库的连接。
        """
        if self.connection:
            self.connection.close()

    def create_table(self):
        """
        在数据库中创建学校信息表。
        """
        with self.connection.cursor() as cursor:
            create_table_query = '''  
            CREATE TABLE IF NOT EXISTS schools (  
                id INT AUTO_INCREMENT PRIMARY KEY,  
                name VARCHAR(255) NOT NULL,  
                schid VARCHAR(50),  
                region VARCHAR(255),  
                authority VARCHAR(255),  
                level VARCHAR(50),  
                rate FLOAT  
            )  
            '''
            cursor.execute(create_table_query)
        self.connection.commit()

    def insert_data(self, all_data):
        """
        向学校信息表中插入数据。

        参数:
        - all_data: 包含所有学校数据的列表。
        """
        with self.connection.cursor() as cursor:
            insert_query = '''  
            INSERT INTO schools (name, schid, region, authority, level, rate)  
            VALUES (%s, %s, %s, %s, %s, %s)  
            '''
            cursor.executemany(insert_query, all_data)
        self.connection.commit()

class UniversitySpider:
    """
    大学信息爬虫类，用于从指定网站爬取大学信息并保存到CSV文件或MySQL数据库。
    """
    def __init__(self):
        self.get_config()
        self.mysql_handler = MySQLHandler(self.mysql_host, self.mysql_user, self.mysql_password, 'university_info')

    def get_config(self, config_file='config.json'):
        """
        从配置文件中读取配置信息。
        """
        try:
            with open(config_file, 'r') as file:

                    config = json.load(file)
        except FileNotFoundError:
                print(f"配置文件{config_file}未找到，请确保文件存在。")
                return
        except json.JSONDecodeError:
                print(f"配置文件{config_file}格式错误，请检查JSON格式是否正确。")
                return
        self.headers = config.get('headers')
        self.csv_file = config.get('csv_file')
        self.get_cookies_sleep_time = config.get('get_cookies_sleep_time')
        self.get_web_sleep_time = config.get('get_web_sleep_time')
        self.cookie_renewal_interval = config.get('cookie_renewal_interval')
        self.mysql_host = config.get('mysql_host')
        self.mysql_user = config.get('mysql_user')
        self.mysql_password = config.get('mysql_password')
        self.mysql_database = config.get('mysql_database')
        self.will_save_to_mysql = config.get('will_save_to_mysql')
        self.will_save_to_csv = config.get('will_save_to_csv')
        self.base_url = ('https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-{start}.dhtml')

    def get_cookies_and_page_from_url(self,url):
        """
        使用浏览器获取网站cookies。
        参数:
        - url: 需要获取cookies的网站URL。
        返回:
        - cookie_jar: 包含获取到的cookies的RequestsCookieJar对象。
        """
        chrome_options = ChromeOptions()
        driver = uc.Chrome(options=chrome_options)

        try:
            driver.get(url)
            time.sleep(self.get_cookies_sleep_time)

            page = driver.find_element(By.XPATH, '//*[@id="PageForm"]/ul/li[8]/a')
            page_info = page.text

            selenium_cookies = driver.get_cookies()
            cookie_jar = RequestsCookieJar()
            for cookie in selenium_cookies:
                cookie_jar.set(cookie['name'], cookie['value'])

            return cookie_jar, page_info
        finally:
            driver.quit()

    def get_cookies_from_url(self,url):
        """
        使用浏览器获取网站cookies。
        参数:
        - url: 需要获取cookies的网站URL。
        返回:
        - cookie_jar: 包含获取到的cookies的RequestsCookieJar对象。
        """
        chrome_options = ChromeOptions()
        driver = uc.Chrome(options=chrome_options)

        try:
            driver.get(url)
            time.sleep(self.get_cookies_sleep_time)

            selenium_cookies = driver.get_cookies()
            cookie_jar = RequestsCookieJar()
            for cookie in selenium_cookies:
                cookie_jar.set(cookie['name'], cookie['value'])

            return cookie_jar
        finally:
            driver.quit()

    def get_url(self, url, cookies):
        """
        发送GET请求到指定URL，并返回响应的HTML内容。

        参数:
        - url: 需要请求的URL。
        - cookies: 用于请求的cookies。

        返回:
        - html_content: 响应的HTML内容。
        """
        try:
            response = requests.get(url, headers=self.headers, cookies=cookies, timeout=5)
            if response.status_code == 200:
                return response.text
            else:
                print(f"请求错误，状态码：{response.status_code}")
        except requests.RequestException as e:
            print(e)
        return None

    def parse_university_info(self,html_content):
        """
        解析HTML内容，提取大学信息。

        参数:
        - html_content: 需要解析的HTML内容。

        返回:
        - university_data: 包含所有大学信息的列表。
        """
        tree = etree.HTML(html_content)
        university_data = []
        for item in tree.xpath('//div[@class="sch-item"]'):

            name = item.xpath('.//a[@class="name js-yxk-yxmc text-decoration-none"]/text()')[0].strip()

            img_element = item.xpath('.//img[starts-with(@src, "https://t1.chei.com.cn/common/xh/")]')[0]
            src_url = img_element.get('src')
            schid = src_url.split('/')[-1].split('.')[0]

            region_and_authority = item.xpath('.//a[@class="sch-department text-decoration-none"]/text()')
            cleaned_list = [item.strip() for item in region_and_authority if item.strip()]
            region = cleaned_list[0]
            authority = cleaned_list[1]

            level = item.xpath('.//a[@class="sch-level text-decoration-none"]/text()[1]')[0].strip()

            rate_elements = item.xpath('.//a[@class="num text-decoration-none"]/text()')
            rate = rate_elements[0].strip() if rate_elements else 'N/A'

            university_data.append([name, schid, region, authority, level, rate])

        return university_data

    def save_to_csv(self, all_data):
        """
        将大学信息保存到CSV文件。

        参数:
        - all_data: 包含所有大学信息的列表。
        """
        now = datetime.now()
        current_date = now.strftime('%Y-%m-%d')
        current_time = now.strftime('%H-%M-%S')

        if '{date}' in self.csv_file and '{time}' in self.csv_file:
            full_csv_file_name = self.csv_file.replace('{date}', current_date).replace('{time}', current_time)
        elif '{date}' in self.csv_file:
            full_csv_file_name = self.csv_file.replace('{date}', current_date)
        elif '{time}' in self.csv_file:
            full_csv_file_name = self.csv_file.replace('{time}', current_time)
        else:
            full_csv_file_name = self.csv_file

        dir_name = os.path.dirname(full_csv_file_name)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print(f"目录 {dir_name} 已创建。")

        print("正在保存数据到CSV文件...")
        with open(full_csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["院校名称","院校代码", "所在地区", "院校主管部门", "院校层次", "院校满意度"])
            writer.writerows(all_data)
        print(f"数据已保存至 {full_csv_file_name}")

    def save_to_mysql(self,all_data):
        """
        将大学信息保存到MySQL数据库。

        参数:
        - MySQLHandle: MySQLHandler实例，用于数据库操作。
        - all_data: 包含所有大学信息的列表。
        """
        print("正在保存数据到MySQL数据库...")
        self.mysql_handler.connect()
        self.mysql_handler.create_table()
        self.mysql_handler.insert_data(all_data)
        self.mysql_handler.disconnect()
        print(f"数据已保存至MySQL数据库 {self.mysql_database}")

    def run(self):
        """
        爬虫的主运行方法，负责爬取数据并保存。
        """
        all_data = []
        cookie_renewal_interval = self.cookie_renewal_interval
        page_count = 0
        cookie, end_of_page = self.get_cookies_and_page_from_url('https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-0.dhtml')
        self.end_of_page = int(end_of_page)
        print(f'即将开始数据爬取，当前需要爬取的总页数是{self.end_of_page}页，保存至mysql的设置为{self.will_save_to_mysql}，保存至csv的设置为{self.will_save_to_csv}')

        for start in range(1, self.end_of_page + 1):
        #for start in range(1, 6):        #仅供测试使用
            url_count = (start - 1) * 20
            url = self.base_url.format(start=url_count)
            html_content = self.get_url(url, cookie)
            if html_content:
                page_data = self.parse_university_info(html_content)
                all_data.extend(page_data)
                print(f"当前进度:{start}/{self.end_of_page}页")
                page_count += 1

                if page_count % cookie_renewal_interval == 0:
                    print(f"爬取{cookie_renewal_interval}页后，重新获取Cookies...")
                    cookie = self.get_cookies_from_url(url)
                    page_count = 0
            else:
                print(f"第{start}页数据抓取失败")

            time.sleep(self.get_web_sleep_time)

        if self.will_save_to_csv == True:
            self.save_to_csv(all_data)
        else:
            print("未启用保存到CSV文件功能")

        if self.will_save_to_mysql == True:
            self.save_to_mysql(all_data)
        else:
            print("未启用保存到MySQL数据库功能")

if __name__ == "__main__":
    Spider = UniversitySpider()
    Spider.run()