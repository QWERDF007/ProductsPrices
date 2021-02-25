import os
import argparse
import logging
import sqlite3
import time
import random

from pprint import PrettyPrinter
from spider import JDSpider


def gen_del_items(pids):
    for pid in pids:
        yield (pid,)


def gen_price_items(prices):
    for pid, price in prices:
        if price is not None:
            price = float(price)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        yield (pid, price, now)


def gen_product_items(products):
    for pid, info in products.items():
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        price = info['price']
        price = None if price < 0 else price
        yield (pid, info['href'], info['shop'], info['name'], price, info['over'], now)


def gen_update_product_items(products):
    for pid, info in products.items():
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        yield (info['price'], info['over'], now, pid)


def record_price(conn, prices):
    sql = "INSERT INTO prices(pid, price, ctime) VALUES(?,?,?)"
    conn.executemany(sql, gen_price_items(prices))
    conn.commit()


def record_products(conn, products):
    sql = "INSERT INTO products(pid,href,shop,name,min,over,mtime) VALUES(?,?,?,?,?,?,?)"
    conn.executemany(sql, gen_product_items(products))
    conn.commit()


def update_products_info(conn, products):
    sql = "UPDATE products SET min = ?, over = ?, mtime = ? WHERE pid = ?"
    conn.executemany(sql, gen_update_product_items(products))
    conn.commit()


def get_db_products_info(conn):
    sql = "SELECT pid, min, over FROM products"
    return conn.execute(sql)


def get_db_products_pid(conn):
    sql = "SELECT pid FROM products"
    return conn.execute(sql)


def gen_update_products(logger, conn, prices):
    products = dict()
    for one in prices:
        pid, price = one
        over = None
        if price is not None:
            price = float(price)
            over = price < 0
        products[pid] = {'price': price, 'over': over}
    db_products = get_db_products_info(conn)
    products_to_update = dict()
    for one in db_products:
        pid, min_price, over = one
        product = products[pid]
        price = product['price']
        if price is not None:
            update = False
            if min_price is None and price > 0:
                min_price = price
                update = True
            elif min_price is not None and 0 < price < min_price:
                min_price = price
                update = True
            if over != product['over']:
                update = True
            if update:
                product['price'] = min_price
                products_to_update[pid] = product
        else:
            logger.warning("pid: {:<16} price is None".format(pid))
    return products_to_update


def add_products(logger, conn, db_pids, logpath):
    pids_to_add = {pid for pid in args['pids']} - db_pids
    total = len(pids_to_add)
    if total > 0:
        spider = JDSpider(logpath)
        products = dict()
        for pid in pids_to_add:
            shop, name, tip = spider.get_shop_name(pid)
            over = True if tip is not None else False
            href = 'https://item.jd.com/' + pid + '.html'
            products[pid] = {"href": href, "shop": shop, "name": name, "over": over, "tip": tip}
        prices = spider.get_prices(pids_to_add)
        for pid, price in prices:
            product = products[pid]
            product['price'] = float(price)
            logger.info("pid: {:<16} shop: {} name: {} price: ￥{} over: {} tip: {}".format(
                pid, product['shop'], product['name'], product['price'], product['over'], product['tip']))
        record_products(conn, products)
        record_price(conn, prices)
        logger.info("add products: {:<8}".format(total))
    else:
        logger.warning("failed to add, args pid size: {:<8} add pids size: {:<8} db pids size: {:<8}".format(
            len(args['pids']), total, len(db_pids)))
        exit(0)


def del_products(logger, conn, db_pids):
    pids_to_del = {pid for pid in args['pids']} & db_pids
    total = len(pids_to_del)
    if total > 0:
        sql = "DELETE FROM products where pid = ?"
        conn.executemany(sql, gen_del_items(pids_to_del))
        conn.commit()
        logger.info("del products: {:<8}".format(total))
    else:
        logger.warning("failed to del, args pids size: {:<8} del pids size: {:<8} db pids size: {:<8}".format(
            len(args['pids']), total, len(db_pids)))
        exit(0)


def query_products(logger, conn, logpath=None, step=25):
    sql = "SELECT pid FROM products"
    db_products = conn.execute(sql)
    pids = [one[0] for one in db_products]
    groups = [pids[i:i + step] for i in range(0, len(pids), step)]
    spider = JDSpider(logpath)
    prices = list()
    for i, pids in enumerate(groups):
        prices += spider.get_prices(pids)
        logger.info("group_{}: {} - {}".format(i, i * 25, (i + 1) * 25))
    record_price(conn, prices)
    for pid, price in prices:
        over = None
        if price is not None:
            over = float(price) < 0
        logger.info("record pid: {:<16} price: ￥{:<16} over: {}".format(pid, price, over))
    products_to_update = gen_update_products(logger, conn, prices)
    update_size = len(products_to_update)
    if update_size > 0:
        update_products_info(conn, products_to_update)
        for pid, product in products_to_update.items():
            logger.info("update pid: {:<16} price: ￥{:<16} over: {}".format(
                pid, product['price'], product['over']))
    logger.info("record products: {:<8} update products: {:<8}".format(len(pids), update_size))


def main(args):
    FMT = "[%(asctime)s] [%(name)s] [%(thread)d] [%(levelname)1.1s] %(message)s"
    logging.basicConfig(filename=args['log'], format=FMT)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Welcome to JD spider.")

    conn = None

    if args['database'] is not None:
        db_path = args['database']
        db_exist = os.path.exists(db_path)
        conn = sqlite3.connect(db_path)
        logger.info("connected to {}".format(db_path))
        if not db_exist:
            conn.executescript(open(args['sql'], 'r', encoding='utf-8').read())
            conn.commit()
            logger.info("create table on {}".format(db_path))

    if args['pids'] is not None:
        db_pids = set()
        if conn is not None:
            db_pids = {one[0] for one in get_db_products_pid(conn)}
        if args['add']:
            add_products(logger, conn, db_pids, args['log'])
        elif args['del']:
            del_products(logger, conn, db_pids)
        else:
            logger.error("What do you want to do?")
            exit(-1)
    else:
        if conn is not None:
            query_products(logger, conn, args['log'])
        else:
            logger.error("can't connect database")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="python JD spider")
    parser.add_argument('-a', '--add', action='store_true', default=False, help='是否添加新商品')
    parser.add_argument('-d', '--del', action='store_true', default=False, help='是否删除商品')
    parser.add_argument('-p', '--pids', type=str, default=None, nargs='+', help='商品id')
    parser.add_argument('-db', '--database', type=str, default='db/products.db', help='存放商品信息的数据库的路径')
    parser.add_argument('-l', '--log', type=str, default=None, help='运行日志路径')
    parser.add_argument('-s', '--sql', type=str, default='db/build_db.sql', help='创建数据库sql')
    args = vars(parser.parse_args())
    PrettyPrinter().pprint(args)
    main(args)
