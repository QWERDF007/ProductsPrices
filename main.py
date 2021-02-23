import argparse
import json
import logging
import requests
import sqlite3

from bs4 import BeautifulSoup
from pprint import PrettyPrinter


headers_name = {
    "referer": "https://mall.jd.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
}

headers_price = {
    "Referer": "https://item.jd.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"
}

PRICE_URL = "https://p.3.cn/prices/mgets"
BASIC_URL = "https://item.jd.com/"
sql = "SELECT pid FROM products"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="python JD spider")
    parser.add_argument('-a', '--add', action='store_true', default=False, help='是否添加新的商品')
    parser.add_argument('-p', '--pids', type=int, default=None, nargs='+', help='商品id')
    parser.add_argument('-db', '--database', type=str, default=None, help='存放商品信息的数据库的路径')
    args = vars(parser.parse_args())
    FMT = "[%(asctime)s] [%(name)s] [%(thread)d] [%(levelname)1.1s] %(message)s"
    logging.basicConfig(format=FMT)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Welcome to JD spider.")
    PrettyPrinter().pprint(args)

    if args['pids'] is None and args['database'] is None:
        logger.critical("no pid/database")
        exit(-1)
    elif args['pids'] is None and args['database'] is not None:
        conn = sqlite3.connect(args['database'])
        c = conn.cursor()
        query_results =  c.execute(sql)
        args['pids'] = [one[0] for one in query_results]

    for pid in args['pids']:
        url = BASIC_URL + str(pid) + '.html'
        res = requests.get(url, headers=headers_name)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'lxml')
            shop_name = soup.find(class_='J-hove-wrap').find(class_='name').text.strip()
            product_name = soup.find(class_='sku-name').text.strip()
            itermover = soup.find(class_='itemover-tip')

            if shop_name is not None:
                logger.info("pid: {:<16} shop name: {}".format(pid, shop_name))
            else:
                logger.warning("pid: {:<16} no shop name".format(pid))
            if product_name is not None:
                logger.info("pid: {:<16} product name: {}".format(pid, product_name))
            else:
                logger.warning("pid: {:<16} no product name".format(pid))
            if itermover is not None:
                logger.critical("pid: {:<16} {}".format(pid, itermover.text.strip()))

        params = {"skuIds": pid}
        res = requests.get(PRICE_URL, headers=headers_price, params=params)
        if res.status_code == 200:
            res = json.loads(res.text)[0]
            price = res.get('p')
            if price is not None:
                logger.info("pid: {:<16} price: {}".format(pid, price))
            else:
                logger.error("pid: {:<16} no price".format(pid))
