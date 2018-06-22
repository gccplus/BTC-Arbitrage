# -*- coding: utf-8 -*-
from bitmex.bitmex_websocket import BitMEXWebsocket
from bitmex.bitmex_rest import bitmex
import logging
import logging.handlers
import time
import json
import requests
import redis


class MyRobot:
    lever = 5
    new_postion_thd = 350
    re_position_thd = 100
    price_table = {
        '8': [4, 10, 21, 43],
        '16': [8, 20, 42, 85],
        '32': [16, 40, 84, 170]
    }
    open_price_list = 'open_price_list'
    base_price_list = 'base_price_list'
    base_price = 'base_price'
    filled_order_set = 'filled_order_set'

    unit_amount_list = 'unit_amount_list'

    def __init__(self):
        self.logger = setup_logger()
        test = False
        api_key = 'Y-sfV-blolT6B9ZMWqUDTmi1'
        api_secret = 'Qtyky7g4QOEC2BzDOf8-t8D3XbswPhLs_D1Ffi4reM_63ZlO'
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        if test:
            url = test_url
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        self.ws = BitMEXWebsocket(endpoint=url, symbol="XBTUSD", api_key=api_key, api_secret=api_secret)

        # init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        self.last_sms_time = 0

    """
    2018/6/14 更新
    每次选取最邻近的订单
    """

    def get_filled_order(self):
        recent_order = None
        for order in self.ws.open_orders():
            if order['ordStatus'] == 'Filled' and order['ordType'] == 'Limit' and (
                    not self.redis_cli.sismember('filled_order_set', order['orderID'])):
                if not recent_order:
                    recent_order = order
                else:
                    if order['timestamp'] > recent_order['timestamp']:
                        recent_order = order
        return recent_order

    def get_delegated_orders(self):
        try:
            orders = self.cli.Order.Order_getOrders(filter=json.dumps({"ordStatus": 'New'})).result()
        except Exception as e:
            self.logger.error('get orders error: %s' % e)
            return []
        else:
            return orders[0]

    def get_ticker(self, symbol):
        # tickers = self.ws.get_ticker()
        while True:
            tickers = self.ws.get_ticker()
            if len(tickers) > 0:
                for ticker in tickers[::-1]:
                    if ticker['symbol'] == symbol:
                        return ticker
            time.sleep(0.5)

    def send_order(self, symbol, side, qty, price, ordtype='Limit'):
        times = 0
        result = 0
        flag = False
        for o in self.get_delegated_orders():
            print('side:%s, price:%s, orderid:%s' % (o['side'], o['price'], o['orderID']))
            if o['side'] == side and o['price'] == price:
                flag = True
                break
        while times < 500:
            self.logger.info('第%s次发起订单委托' % (times + 1))
            if ordtype == 'Limit':
                if flag:
                    self.logger.info('委托已存在')
                    result = 1
                    break
                try:
                    order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty, price=price,
                                                     ordType=ordtype).result()
                except Exception as e:
                    self.logger.error('订单error: %s,1秒后重试' % e)
                    time.sleep(1)
                else:
                    # print(order)
                    self.logger.info('委托成功')
                    result = order[0]['orderID']
                    break
            else:
                try:
                    order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty, ordType=ordtype).result()
                except Exception as e:
                    self.logger.error('订单error: %s,1秒后重试' % e)
                    time.sleep(1)
                else:
                    # print(order)
                    result = order[0]['orderID']
                    break
            times += 1
        return result

    def candel_order(self, orderid):
        times = 0
        result = False
        while times < 500:
            self.logger.info('第%s次发起撤销委托, orderId: %s' % (times + 1, orderid))
            try:
                self.cli.Order.Order_cancel(orderID=orderid).result()
            except Exception as e:
                self.logger.error('撤销错误: %s, 1秒后重试' % e)
                time.sleep(1)
            else:
                # print(order)
                result = True
                break
            times += 1
        return result

    def ament_order(self, orderid, qty):
        times = 0
        while times < 500:
            self.logger.info('第%s次修改订单信息, orderID: %s' % (times + 1, orderid))
            try:
                self.cli.Order.Order_amend(orderID=orderid, orderQty=qty).result()
            except Exception as e:
                logging.error('修改订单错误: %s' % e)
            else:
                self.logger.info('修改成功')
                break
            times += 1

    def close_postion(self, price, unit_amount):
        last_open_price = float(self.redis_cli.lindex('open_price_list', -1))
        if last_open_price < price:
            # XBTUSD市价平仓
            self.logger.info('XBTUSD市价平仓')
            orderid = self.send_order('XBTUSD', 'Buy', unit_amount * 2, 0, 'Market')
            times = 0
            while times < 200:
                self.logger.info('第%s次查询订单状态' % (times + 1))
                order_info = self.cli.Order.Order_getOrders(filter=json.dumps({"orderID": orderid})).result()
                print(order_info)
                if len(order_info[0]) > 0 and order_info[0][0]['ordStatus'] == 'Filled':
                    self.logger.info('XBTUSD完成市价委托')
                    break
                times += 1
                time.sleep(1)
            self.logger.info('XBTU18市价平仓')
            orderid = self.send_order('XBTU18', 'Sell', unit_amount * 2, 0, 'Market')
            times = 0
            while times < 200:
                self.logger.info('第%s次查询订单状态' % (times + 1))
                order_info = self.cli.Order.Order_getOrders(filter=json.dumps({"orderID": orderid})).result()
                # print(order_info)
                if len(order_info[0]) > 0 and order_info[0][0]['ordStatus'] == 'Filled':
                    self.logger.info('XBTU18完成市价委托')
                    self.logger.info('rpop open_price')
                    last_open_price = self.redis_cli.rpop('open_price_list')
                    self.redis_cli.rpop('base_price_list')
                    #
                    self.logger.info('撤销多余委托')
                    for o in self.get_delegated_orders():
                        if o['price'] < last_open_price and o['price'] == 'Buy':
                            self.logger.info('cancel order orderID: %s, price: %s' % (o['orderID'], o['price']))
                            self.candel_order(o['orderID'])
                    break
                times += 1
                time.sleep(1)

    def sms_notify(self, msg):
        if int(time.time() - self.last_sms_time > 900):
            self.logger.info('短信通知: %s' % msg)
            url = 'http://221.228.17.88:8080/sendmsg/send'
            params = {
                'phonenum': '18118999630',
                'msg': msg
            }
            requests.get(url, params=params)
            self.last_sms_time = int(time.time())

    def adjust_price(self, price):
        import re
        match = re.match(r"(\d+)\.(\d{2})", '%.2f' % price)
        if match:
            integer = int(match.group(1))
            decimal = int(match.group(2))
            if decimal < 25:
                decimal = 0
            elif decimal < 75:
                decimal = 0.5
            else:
                decimal = 1
            return integer + decimal
        else:
            return price

    def get_current_index(self, price):
        if self.redis_cli.llen('open_price_list') == 0:
            index = -1
        elif self.redis_cli.llen('open_price_list') == 1:
            index = 0
        elif self.redis_cli.llen('open_price_list') == 2:
            if price > float(self.redis_cli.lindex('open_price_list', 1)):
                index = 0
            else:
                index = 1
        else:
            if price > float(self.redis_cli.lindex('open_price_list', 1)):
                index = 0
            elif price > float(self.redis_cli.lindex('open_price_list', 2)):
                index = 1
            else:
                index = 2
        return index

    def test_market_order(self):
        order = self.send_order('XBTU18', 'Buy', 1, 0, 'Market')
        print(type(order))
        print(order[0]['order'])

    def test_order_info(self):
        orderid = '366a1657-a21b-ca63-cc0b-fabe577fd42c'
        order_info = self.cli.Order.Order_getOrders(filter=json.dumps({"orderID": orderid})).result()
        print(order_info)

    def test_limit_order(self):
        self.send_order('XBTU18', 'Buy', 44, 7000, 'Limit')

    def test_redis(self):
        self.redis_cli.lpush('open_price_list', '111.23')
        if self.redis_cli.llen('open_price_list') > 0:
            print(type(self.redis_cli.lindex('open_price_list', -1)))
            print(float(self.redis_cli.lindex('open_price_list', -1)))

    def get_unfilled_order(self):
        orders = self.get_delegated_orders()
        print(orders[0])
        print(orders[1])
        print(orders[40]['timestamp'] > orders[8]['timestamp'])

    def test_amend_order(self):
        print(dir(self.cli.Order))
        orderID = '07eb8afe-3a72-1e4d-adce-e4106f030559'
        orderQty = 304
        times = 0
        while times < 50:
            self.logger.info('第%s次修改订单信息, orderID: %s' % (times + 1, orderID))
            try:
                order = self.cli.Order.Order_amend(orderID=orderID, orderQty=orderQty).result()
            except Exception as e:
                logging.error('修改订单错误: %s' % e)
            else:
                print(order)
                break
            times += 1


def setup_logger():
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    #robot = MyRobot()
    import os
    print(os.name)
