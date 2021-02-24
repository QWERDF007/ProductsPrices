import os
import argparse
import logging
import sqlite3

from pprint import PrettyPrinter
from spider import JDSpider


def main(args):
    FMT = "[%(asctime)s] [%(name)s] [%(thread)d] [%(levelname)1.1s] %(message)s"
    logging.basicConfig(filename=args['log'], format=FMT)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("Welcome to JD spider.")
    PrettyPrinter().pprint(args)

    conn = None
    c = None

    if args['database'] is not None:
        db_path = args['database']
        db_exist = os.path.exists()
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        logger.info("connected to {}".format(db_path))
        if not db_exist:
            c.executescript(args['sql'])
            conn.commit()
            logger.info("create database on {}".format(db_path))

    if args['pids'] is not None:
        if args['add']:
            pass
        elif args['del']:
            pass
        else:
            logger.error("What do you want to do?")
            exit(-1)
    else:
        if conn is not None:
            sql = "SELECT pid FROM products"
            query_results = c.execute(sql)
            if len(query_results) > 0:
                pass
            else:
                logger.error("can't find any pid in database")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="python JD spider")
    parser.add_argument('-a', '--add', action='store_true', default=False, help='是否添加新商品')
    parser.add_argument('-d', '--del', action='store_true', default=False, help='是否删除商品')
    parser.add_argument('-p', '--pids', type=int, default=None, nargs='+', help='商品id')
    parser.add_argument('-db', '--database', type=str, default='db/products.db', help='存放商品信息的数据库的路径')
    parser.add_argument('-l', '--log', type=str, default=None, help='运行日志路径')
    parser.add_argument('-s', '--sql', type=str, default='db/build_db.spl', help='创建数据库sql')
    args = vars(parser.parse_args())
    main(args)
