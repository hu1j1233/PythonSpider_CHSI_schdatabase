# 导入必要的库
import requests
from lxml import etree
import csv
import undetected_chromedriver as uc
#from selenium.webdriver.chrome.options import Options
from undetected_chromedriver.options import ChromeOptions
import time
from requests.cookies import RequestsCookieJar
import pymysql

"""
学信网院校数据爬虫
作者：Huiji
邮箱：huiji233@gmail.com
运行环境:python3.9
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
        self.connection = None

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
                school_name VARCHAR(255) NOT NULL,  
                school_code VARCHAR(50),  
                region VARCHAR(255),  
                department VARCHAR(255),  
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
            INSERT INTO schools (school_name, school_code, region, department, level, rate)  
            VALUES (%s, %s, %s, %s, %s, %s)  
            '''
            cursor.executemany(insert_query, all_data)
        self.connection.commit()

class UniversityCrawler:
    """
    大学信息爬虫类，用于从指定网站爬取大学信息并保存到CSV文件或MySQL数据库。
    """
    def __init__(self):
        """
        初始化爬虫类的属性，如请求头、CSV文件名、基础URL、mysql数据库连接信息等。
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7'}
        self.csv_file = 'universities_data.csv'
        self.base_url = "https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-{start}.dhtml"
        self.get_cookies_sleep_time = 3                         # 获取cookies的等待时间
        self.get_web_sleep_time = 5                             # 获取网页的等待时间
        self.cookie_renewal_interval = 5                        # cookies更新间隔
        self.mysql_host = 'localhost'                           # mysql数据库主机地址
        self.mysql_user = 'root'                                # mysql数据库用户名
        self.mysql_password = '123456'                          # mysql数据库密码
        self.mysql_database = 'university_data'                 # mysql数据库名称
        self.end_of_page = 145                                  # 结束页码
        self.auto_get_end_of_page = False                       # 是否自动获取结束页码    #未实现
        self.will_save_to_mysql = False                         # 是否保存到mysql       #未实现
        self.will_save_to_csv = False                           # 是否保存到CSV文件      #未实现

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
    def fetch_url(self, url, cookies):
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
        with open(self.csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["院校名称","院校代码", "所在地区", "院校主管部门", "院校层次", "院校满意度"])
            writer.writerows(all_data)
        print(f"数据已保存至 {self.csv_file}")

    def save_to_mysql(self,MySQLHandle,all_data):
        """
        将大学信息保存到MySQL数据库。

        参数:
        - MySQLHandle: MySQLHandler实例，用于数据库操作。
        - all_data: 包含所有大学信息的列表。
        """
        mysql_handler = MySQLHandler(self.mysql_host,self.mysql_user,self.mysql_password,self.mysql_database)
        mysql_handler.connect()
        mysql_handler.create_table()
        mysql_handler.insert_data(all_data)
        mysql_handler.disconnect()

    def run(self):
        """
        爬虫的主运行方法，负责爬取数据并保存。
        """
        all_data = []
        cookie_renewal_interval = self.cookie_renewal_interval
        page_count = 0
        cookie = self.get_cookies_from_url('https://gaokao.chsi.com.cn/sch/search--ss-on,option-qg,searchType-1,start-0.dhtml')

        for start in range(1, (self.end_of_page) +1 ):
            url = self.base_url.format(start=start)
            html_content = self.fetch_url(url, cookie)
            if html_content:
                page_data = self.parse_university_info(html_content)
                all_data.extend(page_data)
                print(f"成功抓取第{start}页数据")
                page_count += 1

                if page_count % cookie_renewal_interval == 0:
                    print(f"爬取{cookie_renewal_interval}页后，重新获取Cookies...")
                    cookie = self.get_cookies_from_url(url)
                    page_count = 0
            else:
                print(f"第{start}页数据抓取失败")

            time.sleep(self.get_web_sleep_time)

        self.save_to_csv(all_data)
        #self.save_to_mysql(all_data)#未启用


if __name__ == "__main__":
    crawler = UniversityCrawler()
    crawler.run()
