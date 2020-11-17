import argparse
import datetime
import os
import sqlite3
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class JDSpider:
    URL = 'https://www.jd.com/'
    JS_TOP = "window.scrollTo(document.body.scrollHeight,0)"
    JS_BOTTOM = "window.scrollTo(0,document.body.scrollHeight)"

    def __init__(self, driver_path):
        """
        JD关键字搜索查询爬虫
        :param driver_path:
        """
        self._opt = webdriver.ChromeOptions()
        self._opt.add_argument("--disable-extensions")
        # self._opt.add_argument("--disable-gpu")
        # self._opt.add_argument("--headless")
        self._driver = webdriver.Chrome(executable_path=driver_path, options=self._opt)
        self._driver.get(JDSpider.URL)

    def __call__(self, name):
        try:
            search_btn = self._driver.find_element_by_id("key")
            search_btn.clear()
            search_btn.send_keys(name)
            search_btn.send_keys(Keys.ENTER)
            wait = WebDriverWait(self._driver, 10)
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "crumbs-nav-item")))
            self._driver.execute_script(JDSpider.JS_BOTTOM)
            time.sleep(2)
            self._driver.execute_script(JDSpider.JS_BOTTOM)
            time.sleep(1)
            goodslist = self._driver.find_element_by_id('J_goodsList')
            goods = goodslist.find_elements_by_xpath('.//ul//li')
            for item in goods:
                pid = item.get_attribute('data-sku')
                pname = item.find_element_by_class_name('p-name-type-2')
                shop = item.find_element_by_class_name('p-shop').text
                p_price = item.find_element_by_class_name('p-price')
                price = p_price.find_element_by_class_name('J_%s' % pid).text[1:]
                icons = item.find_element_by_class_name('p-icons')
                if "自营" in icons.text and shop:
                    href = pname.find_element_by_xpath('.//a').get_attribute('href')
                    print(pname.text)
                    print(name, pid, shop, price, href, datetime.date.today().strftime("%Y-%m-%d"))
                    print()
        except Exception as e:
            print(e)
        finally:
            self._driver.close()


class ProductSpider:
    URL = 'https://item.jd.com'

    def __init__(self, driver_path: str):
        """
        JD根据pid查询价格爬虫
        :param driver_path:
        """
        self._options = webdriver.ChromeOptions()
        # self._options.add_argument("--disable-extensions")
        self._options.add_argument("--disable-gpu")
        self._options.add_argument("--headless")
        self._options.add_argument('--no-sandbox')
        prefs = {"profile.managed_default_content_settings.images": 2}
        self._options.add_experimental_option("prefs", prefs) # disable images
        self._driver = webdriver.Chrome(executable_path=driver_path, options=self._options)

    def __call__(self, pid: int):
        try:
            href = '{}/{}.html'.format(self.URL, pid)
            self._driver.get(href)
            wait = WebDriverWait(self._driver, 30)
            p_price = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "p-price")))
            price = float(p_price.text[1:])
            # name = self._driver.find_element_by_class_name("ellipsis").text
            name = self._driver.find_element_by_class_name("sku-name").text.strip()
            shop = self._driver.find_element_by_class_name("J-hove-wrap").find_element_by_class_name("name").text
            if not shop:
                shop = None
            return pid, href, shop, name, price, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(e)

    def close(self):
        self._driver.close()


def update_products_info(conn, infos):
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM products')
    to_update = dict()
    for one in cursor.fetchall():
        pid = one[0]
        is_update = False
        minimum = one[-1]
        for v in one[1:]:
            if not v:
                is_update = True
                break
        to_update[str(pid)] = {"minimum": minimum, "is_update": is_update}
    params = list()
    for info in infos:
        pid = info[0]
        price = info[-1]
        minimum = to_update[str(pid)]["minimum"]
        is_update = to_update[str(pid)]["is_update"]
        if is_update:
            minimum = price
        else:
            if minimum:
                if price < minimum:
                    minimum = price
            else:
                minimum = price
        params.append(tuple([*info[1:-1], minimum, pid]))
    if params:
        cursor.executemany('UPDATE products SET href = ?, shop = ?, name = ?, min = ? WHERE pid = ?', params)
        conn.commit()
        
def update(conn, pids, chromedriver):
    cursor = conn.cursor()
    spider = ProductSpider(chromedriver)
    prices = list()
    products = list()
    for pid in pids:
        pid, *product_info, price, date = spider(pid)
        prices.append(tuple([pid, price, date]))
        products.append([pid, *product_info, price])
    spider.close()
    cursor.executemany('INSERT INTO prices(pid, price, ctime) VALUES(?,?,?)', prices)
    conn.commit()
    update_products_info(conn, products)
    print('num prices: {}, num products: {}'.format(len(prices), len(products)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pids', type=int, default=None, nargs='+', help='product id')
    parser.add_argument('-a', '--add', default=False, action='store_true', help='add pid or not')
    parser.add_argument('-d', '--db', type=str, default='/home/wt/ProductsPrice/db/products.db', help='path to database')
    parser.add_argument('-c', '--chromedriver', type=str, default='/usr/bin/chromedriver', help='path to chromedriver')
    args = vars(parser.parse_args())
    conn = None
    cursor = None
    pids = set()
    if not os.path.exists(args["db"]):
        print("create products")
        conn = sqlite3.connect(args["db"])
        cursor = conn.cursor()
        cursor.executescript(open('./sql/create_tables.sql', 'r').read())
        conn.commit()
    else:
        conn = sqlite3.connect(args["db"])
        cursor = conn.cursor()

    cursor.execute('SELECT pid FROM products')
    for one in cursor.fetchall():
        pids.add(one[0])

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if args['add'] and args["pids"]:
        params = list()
        for pid in args["pids"]:
            if pid not in pids:
                pids.add(pid)
                params.append(tuple([pid]))
        if len(params) > 0:
            cursor.executemany('INSERT INTO products(pid) VALUES(?)', params)
            conn.commit()
            pids = {param[0] for param in params}
            update(conn, pids, args["chromedriver"])
    elif len(pids) > 0:
        update(conn, pids, args["chromedriver"])
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))        
    print('-'*20)
    print()
