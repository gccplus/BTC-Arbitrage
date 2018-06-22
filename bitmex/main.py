# -*- coding: utf-8 -*-
from bitmex.bitmex_websocket import BitMEXWebsocket
from bitmex.bitmex_rest import bitmex
import logging
import logging.handlers
import time
import json
import requests
import redis
import os


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
        api_key = os.getenv('BITMEX_API_KEY')
        api_secret = os.getenv('BITMEX_API_SECRET')
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

    def run(self):
        self.logger.info('start')
        while True:
            filled_order = self.get_filled_order()
            if filled_order:
                cum_qty = filled_order['cumQty']
                # 修改成委托价格而不是成交价格
                avg_px = filled_order['price']
                side = filled_order['side']
                self.logger.info('----------------------------------------------------------------------')
                self.logger.info(
                    'side: %s, cum_qty: %s, avg_px: %s, orderID: %s' % (side, cum_qty, avg_px, filled_order['orderID']))
                index = self.get_current_index(avg_px)
                price_base = 8
                unit_amount = 0
                if index >= 0:
                    price_base = int(self.redis_cli.lindex('base_price_list', index))
                    unit_amount = int(self.redis_cli.lindex('unit_amount_list', index))
                    self.logger.info('index: %s, price_base: %s, unit_amount: %s' % (index, price_base, unit_amount))
                price_table = self.price_table[str(price_base)]

                if filled_order['symbol'] == 'XBTU18' and side == 'Buy':
                    if cum_qty % 16 == 0:
                        self.logger.info('XBTU18卖出: %s,价格: %s' % (cum_qty * 2, avg_px + price_table[3]))
                        orderid = self.send_order('XBTU18', 'Sell', cum_qty * 2, avg_px + price_table[3])
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                    elif cum_qty % 2 == 0:
                        if cum_qty % 8 == 0:
                            price_buy = avg_px - price_base * 8
                            price_sell = avg_px + price_table[2]
                        elif cum_qty % 4 == 0:
                            price_buy = avg_px - price_base * 4
                            price_sell = avg_px + price_table[1]
                        else:
                            price_buy = avg_px - price_base * 2
                            price_sell = avg_px + price_table[0]
                        self.logger.info('XBTU18卖出: %s,价格: %s' % (cum_qty, price_sell))
                        orderid = self.send_order('XBTU18', 'Sell', cum_qty, price_sell)
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break

                        self.logger.info('XBTU18买入: %s,价格: %s' % (cum_qty * 2, price_buy))
                        orderid = self.send_order('XBTU18', 'Buy', cum_qty * 2, price_buy)
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                    else:
                        self.logger.info('cum_qty异常')
                        break

                elif filled_order['symbol'] == 'XBTU18' and side == 'Sell':
                    if cum_qty % 32 == 0:
                        self.logger.info('XBTUSD市价平仓: %s' % (unit_amount * 2))
                        if self.send_order('XBTUSD', 'Buy', unit_amount * 2, 0, 'Market') == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                        #
                        self.logger.info('撤销多余Sell委托')
                        open_price = float(self.redis_cli.lindex('open_price_list', index))
                        self.logger.info('open price: %s' % open_price)
                        for o in self.get_delegated_orders():
                            print(o)
                            if o['side'] == 'Sell' and avg_px < o['price'] < open_price:
                                self.logger.info('cancel order orderID: %s, price: %s' % (o['orderID'], o['price']))
                                self.candel_order(o['orderID'])
                        # 平仓
                        self.close_postion(avg_px, unit_amount)
                        #
                        self.logger.info('rpop')
                        self.redis_cli.rpop('open_price_list')
                        self.redis_cli.rpop('base_price_list')
                        self.redis_cli.rpop('unit_amount_list')
                        self.logger.info('短信通知，已全部平仓，待重建仓位')
                        self.sms_notify('重建仓位')
                    elif cum_qty % 2 == 0:
                        if cum_qty % 16 == 0:
                            buy_price = avg_px - price_table[3]
                            self.close_postion(avg_px, unit_amount)
                        elif cum_qty % 8 == 0:
                            buy_price = avg_px - price_table[2]
                        elif cum_qty % 4 == 0:
                            buy_price = avg_px - price_table[1]
                        else:
                            buy_price = avg_px - price_table[0]
                        self.logger.info('XBTU18买入: %s,价格: %s' % (cum_qty, buy_price))
                        orderid = self.send_order('XBTU18', 'Buy', cum_qty, buy_price)
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                    else:
                        self.logger.info('cum_qty异常')
                        break
                elif filled_order['symbol'] == 'XBTUSD' and side == 'Sell':
                    # 市价买入XBTU18
                    orderid = self.send_order('XBTU18', 'Buy', cum_qty, 0, 'Market')
                    times = 0
                    while times < 20:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        order_info = self.cli.Order.Order_getOrders(filter=json.dumps({"orderID": orderid})).result()
                        # print(order_info)
                        if len(order_info[0]) > 0 and order_info[0][0]['ordStatus'] == 'Filled':
                            cum_qty = order_info[0][0]['cumQty']
                            avg_px = self.adjust_price(order_info[0][0]['avgPx'])
                            self.logger.info('rpush')
                            self.redis_cli.rpush('open_price_list', avg_px)
                            if self.redis_cli.get('base_price') and self.redis_cli.get('unit_amount'):
                                self.redis_cli.rpush('base_price_list', self.redis_cli.get('base_price'))
                                self.redis_cli.rpush('unit_amount_list', self.redis_cli.get('unit_amount'))
                            else:
                                self.logger.info('base_price或者unit_amount未赋值')
                                time.sleep(300)
                            #
                            llen = int(self.redis_cli.llen('open_price_list'))
                            if llen > 1:
                                price = float(self.redis_cli.lindex('open_price_list', llen - 2))
                                for o in self.get_delegated_orders():
                                    if o['orderQty'] % 32 == 0 and o['side'] == 'Sell' and o['price'] < price:
                                        self.ament_order(o['orderID'], o['orderQty'] / 2)
                            price_base = float(self.redis_cli.get('base_price'))
                            self.logger.info('cum_qty: %s, avg_px: %s' % (cum_qty, avg_px))
                            self.logger.info('XBTU18买入: %s,价格: %s' % (cum_qty, avg_px - price_base))
                            orderid = self.send_order('XBTU18', 'Buy', cum_qty, avg_px - price_base)
                            if orderid == 0:
                                self.logger.info('委托失败，程序终止')
                            break

                        times += 1
                        time.sleep(1)
                elif filled_order['symbol'] == 'XBTUSD' and side == 'Buy':
                    orderid = self.send_order('XBTU18', 'Sell', cum_qty, 0, 'Market')
                    times = 0
                    while times < 20:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        order_info = self.cli.Order.Order_getOrders(filter=json.dumps({"orderID": orderid})).result()
                        # print(order_info)
                        if len(order_info[0]) > 0 and order_info[0][0]['ordStatus'] == 'Filled':
                            self.logger.info('XBTU18完成市价委托')
                            self.logger.info('rpop')
                            open_price = float(self.redis_cli.rpop('open_price_list'))
                            self.redis_cli.rpop('base_price_list')
                            self.redis_cli.rpop('unit_amount_list')
                            #
                            for o in self.get_delegated_orders():
                                if o['price'] < open_price and o['side'] == 'Buy':
                                    self.logger.info(
                                        'cancel order, orderID: %s, price: %s' % (o['orderID'], o['price']))
                                    self.candel_order(o['orderID'])
                            break
                        times += 1
                        time.sleep(1)
                #
                self.redis_cli.sadd('filled_order_set', filled_order['orderID'])

            if self.redis_cli.llen('open_price_list') > 0:
                ticker = self.get_ticker('XBTU18')
                bid_price = ticker['bidPrice']
                last_open_price = float(self.redis_cli.lindex('open_price_list', -1))

                if bid_price - last_open_price < -1 * self.new_postion_thd:
                    # self.logger.info('短信通知，开启新的仓位')
                    self.sms_notify(
                        '开启新的仓位 bid_price: %s, last_open_price: %s' % (
                            bid_price, self.redis_cli.lindex('open_price_list', -1)))
                #
                if bid_price - last_open_price > self.re_position_thd:
                    # self.logger.info('短信通知，重建仓位')
                    self.sms_notify(
                        '重建仓位 bid_price: %s, last_open_price: %s' % (
                            bid_price, self.redis_cli.lindex('open_price_list', -1)))

            time.sleep(0.2)


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
    robot = MyRobot()
    robot.run()
