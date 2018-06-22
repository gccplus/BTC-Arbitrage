# -*- coding: utf-8 -*-
from binance.client import Client as BinanceSpot
from binance.websockets import BinanceSocketManager
import time
import logging.handlers
import sys
import re

logger = logging.getLogger("triangle")
# logging.basicConfig()

# 指定logger输出格式
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

# 文件日志
file_handler = logging.handlers.TimedRotatingFileHandler('log', when='midnight')
file_handler.suffix = "%Y-%m-%d"
file_handler.setFormatter(formatter)

# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter

# 为logger添加的日志处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.setLevel(logging.INFO)

"""
三角套利，平台币安
"""


class TriangleArbitrage:
    symbols = [
        'adausdt', 'btcusdt', 'ltcusdt', 'bccusdt', 'neousdt', 'qtumusdt', 'ethusdt',
        'adabtc', 'bccbtc', 'ltcbtc', 'neobtc', 'qtumbtc', 'ethbtc'
    ]
    fee_rate = 0.0005
    profit_rate = 0.0001
    slippage = 0.0002

    def __init__(self):
        key_dict = {}
        # 读取配置文件
        with open('config', 'r') as f:
            for line in f.readlines():
                splited = line.split('=')
                if len(splited) == 2:
                    key_dict[splited[0].strip()] = splited[1].strip()

        # self.output = open('history', 'a+')
        self.binanceClient = BinanceSpot(key_dict['BINANCE_ACCESS_KEY2'], key_dict['BINANCE_SECRET_KEY2'])
        # websocket binance
        bm = BinanceSocketManager(self.binanceClient)
        # bm.start_depth_socket('BTCUSDT', self.on_binance_message)
        bm.start_multiplex_socket(['{}@depth10'.format(item) for item in self.symbols], self.on_binance_message)
        # bm.start_depth_socket('BTCUSDT', self.on_binance_message, depth=BinanceSocketManager.WEBSOCKET_DEPTH_10)
        bm.start_user_socket(self.on_binance_message)
        bm.start()

        #
        self.depth = {}
        self.depth_last_update = {}
        self.exchange_info = {}
        self.balance = {}
        self.bn_execution_report = []

    def on_binance_message(self, msg):
        if msg.has_key('stream'):
            self.depth.__setitem__(msg['stream'][:-8].upper(), msg['data'])
        elif msg['e'] == 'executionReport':
            print msg
            self.bn_execution_report.append(msg)
        else:
            pass

    def update_account_info(self):
        try:
            account_info = self.binanceClient.get_account()
        except Exception as e:
            logger.error('Binance get_account error: %s' % e)
        else:
            for info in account_info['balances']:
                self.balance.__setitem__(info['asset'], {
                    'free': float(info['free']),
                    'freezed': float(info['locked'])
                })
            # print self.balance

    def get_exchange_info(self):
        try:
            exchange_info = self.binanceClient.get_exchange_info()
        except Exception as e:
            logger.error('get_exchange_info error: %s' % e)
        else:
            # print exchange_info
            for symbol in exchange_info['symbols']:
                if symbol['symbol'].lower() in self.symbols:
                    self.exchange_info.__setitem__(symbol['symbol'], {
                        'price_filter': symbol['filters'][0]['tickSize'].index('1') - 1,
                        'lot_size': symbol['filters'][1]['minQty'].index('1') - 1,
                        'min_notional': float(symbol['filters'][2]['minNotional'])
                    })
            print self.exchange_info

    @staticmethod
    def cut2_float(s, n):
        if isinstance(s, float):
            s = '{:.8f}'.format(s)
        if n > 0:
            pattern = re.compile(r'(\d+\.\d{1,%d})\d*' % n)
        else:
            pattern = re.compile(r'(\d+)\.\d*')
        return float(pattern.match(s).group(1))

    def merge_depth(self, depth, symbol):
        new_depth = []
        price_filter = self.exchange_info.get(symbol)['price_filter']
        for d in depth:
            price = TriangleArbitrage.cut2_float(d[0], price_filter - 1)
            amount = float(d[1])
            if len(new_depth) == 0:
                new_depth.append([price, amount])
            else:
                if new_depth[-1][0] == price:
                    new_depth[-1] = [price, new_depth[-1][1] + amount]
                else:
                    new_depth.append([price, amount])
        return new_depth[:3]

    def test_binance_trade(self):
        price = 20.803
        # buy_order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=0.001011,
        #                                                price=price, newOrderRespType='FULL')
        amount = 0.680
        order = self.binanceClient.order_limit_buy(symbol='QTUMUSDT', quantity=amount,
                                                   price=price, newOrderRespType='FULL')
        print order

    def calc_exchange_amount(self, d1, d2, d3):
        amount1, amount2, amount3 = 0, 0, 0
        for d in d1:
            if abs(d[0] - d1[0][0]) / d1[0][0] <= self.slippage / 2:
                amount1 += d[1]
        for d in d2:
            if abs(d[0] - d2[0][0]) / d2[0][0] <= self.slippage:
                amount2 += d[1]
        amount2 /= 2
        for d in d3:
            if abs(d[0] - d3[0][0]) / d3[0][0] <= self.slippage:
                amount3 += d[1]
        amount3 /= 2
        return min(amount1, amount2 * d2[0][0] / d1[0][0], amount3 * d3[0][0])

    def go(self):
        print 'go'
        time.sleep(5)
        for symbol in ['bcc', 'ltc', 'neo', 'eth', 'qtum']:
            symbol1 = 'BTCUSDT'
            symbol2 = '{}usdt'.format(symbol).upper()
            symbol3 = '{}btc'.format(symbol).upper()
            self.depth_last_update.__setitem__(symbol1, self.depth[symbol1]['lastUpdateId'])
            self.depth_last_update.__setitem__(symbol2, self.depth[symbol2]['lastUpdateId'])
            self.depth_last_update.__setitem__(symbol3, self.depth[symbol3]['lastUpdateId'])
        while True:
            flag = False
            for symbol in ['bcc', 'ltc', 'neo', 'ada', 'eth']:
                symbol1 = 'BTCUSDT'
                symbol2 = '{}usdt'.format(symbol).upper()
                symbol3 = '{}btc'.format(symbol).upper()

                while True:
                    depth1 = self.depth[symbol1]
                    depth2 = self.depth[symbol2]
                    depth3 = self.depth[symbol3]
                    print depth3
                    if depth1['lastUpdateId'] <= self.depth_last_update.get(symbol1) or \
                            depth2['lastUpdateId'] <= self.depth_last_update.get(symbol2) or \
                            depth3['lastUpdateId'] <= self.depth_last_update.get(symbol3):
                        # print '等待深度更新'
                        time.sleep(0.1)
                        continue
                    self.depth_last_update[symbol1] = depth1['lastUpdateId']
                    self.depth_last_update[symbol2] = depth2['lastUpdateId']
                    self.depth_last_update[symbol3] = depth3['lastUpdateId']
                    a = (float(depth2['bids'][0][0]) / float(depth1['asks'][0][0]) - float(
                        depth3['asks'][0][0])) / float(depth3['asks'][0][0])

                    b = (float(depth3['bids'][0][0]) - float(depth2['asks'][0][0]) / float(
                        depth1['bids'][0][0])) / float(depth3['bids'][0][0])

                    print a if a > b else b
                    # 赚USDT
                    if a > self.fee_rate * 3 + self.profit_rate:
                        # 合并深度
                        print depth1
                        print depth2
                        print depth3
                        ask_1 = [(float(i[0]), float(i[1])) for i in depth1['asks']]
                        bid_2 = [(float(i[0]), float(i[1])) for i in depth2['bids']]
                        ask_3 = [(float(i[0]), float(i[1])) for i in depth3['asks']]

                        lot_size = self.exchange_info.get(symbol3)['lot_size']
                        price_filter = self.exchange_info.get(symbol3)['price_filter']
                        min_notional = self.exchange_info.get(symbol3)['min_notional']
                        amount = TriangleArbitrage.cut2_float(
                            min(self.calc_exchange_amount(ask_1, bid_2, ask_3),
                                0.015 * bid_2[0][0]),
                            lot_size)

                        order_price = round(ask_3[0][0] * (1 + self.slippage), price_filter)
                        btc_amount = amount * order_price
                        print symbol3, price_filter, amount, order_price, btc_amount
                        if btc_amount > self.balance['BTC']['free']:
                            btc_amount = self.balance['BTC']['free']
                            amount = TriangleArbitrage.cut2_float(btc_amount / ask_3[0][0] / (1 + self.slippage * 2),
                                                                  lot_size)

                        if amount < 10 ** (-1 * lot_size):
                            logger.info('%s数量低于 %s' % (symbol, 10 ** (-1 * lot_size)))
                            continue

                        if btc_amount < min_notional:
                            logger.info('BTC数量小于 %s, 本单取消' % self.exchange_info.get(symbol3)['min_notional'])
                            continue

                        # p3买入
                        logger.info('买入:%s' % symbol3)
                        try:
                            order = self.binanceClient.order_limit_buy(symbol=symbol3, quantity=amount,
                                                                       price=order_price)
                        except Exception as e:
                            logger.error(u'%s买入错误: %s' % (symbol3, e))
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('buy orderId: %s, state: %s' % (order_id, order['status']))
                        field_cash_amount = 0
                        field_amount = 0
                        if order['status'] == 'NEW' or order['status'] == 'PARTIALLY_FILLED':
                            logger.info('撤消未完成委托')
                            try:
                                cancel_r = self.binanceClient.cancel_order(symbol=symbol3, orderId=order_id)
                                logger.info('撤销成功')
                                print cancel_r
                            except Exception as e:
                                logger.error(u'撤销错误: %s' % e)

                        logger.info('更新成交量')
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'CANCELED' or current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))

                        amount2 = field_amount
                        amount1 = field_cash_amount

                        # p2卖出
                        logger.info('卖出: %s' % symbol2)
                        lot_size = self.exchange_info.get(symbol2)['lot_size']
                        price_filter = self.exchange_info.get(symbol2)['price_filter']
                        amount = TriangleArbitrage.cut2_float(amound2, lot_size)
                        order_price = round(bid_2[0][0] * (1 - 0.005), price_filter)
                        if amount < 10 ** (-1 * lot_size):
                            logger.info('卖出数量低于 %s, 本单取消' % (10 ** (-1 * lot_size)))
                            continue
                        try:
                            order = self.binanceClient.order_limit_sell(symbol=symbol2, quantity=amount,
                                                                        price=order_price)
                        except Exception as e:
                            logger.error(u'%s卖出错误: %s' % (symbol2, e))
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('sell orderId: %s, state: %s' % (order_id, order['status']))
                        logger.info('更新成交量')
                        field_cash_amount = 0
                        field_amount = 0
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))

                        # p1买入
                        logger.info('买入: %s' % symbol1)
                        lot_size = self.exchange_info.get(symbol1)['lot_size']
                        price_filter = self.exchange_info.get(symbol1)['price_filter']
                        amount = round(amount1, price_filter)
                        order_price = round(ask_1[0][0] * (1 + 0.005), price_filter)

                        if amount < 10 ** (-1 * lot_size):
                            logger.info('卖出数量低于 %s, 本单取消' % (10 ** (-1 * lot_size)))
                            continue
                        try:
                            order = self.binanceClient.order_limit_buy(symbol=symbol1, quantity=amount,
                                                                       price=order_price)
                        except Exception as e:
                            logger.error(u'%s卖出错误: %s' % (symbol1, e))
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('buy orderId: %s, state: %s' % (order_id, order['status']))
                        logger.info('更新成交量')
                        field_cash_amount = 0
                        field_amount = 0
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))
                        self.update_account_info()
                        flag = True
                    # 赚BTC
                    elif b > self.fee_rate * 3 + self.profit_rate:
                        # 保持USDT数量不变,赚取BTC
                        bid_1 = [(float(i[0]), float(i[1])) for i in depth1['bids']]
                        ask_2 = [(float(i[0]), float(i[1])) for i in depth2['asks']]
                        bid_3 = [(float(i[0]), float(i[1])) for i in depth3['bids']]

                        print depth1
                        print depth2
                        print depth3

                        lot_size = self.exchange_info.get(symbol1)['lot_size']
                        price_filter = self.exchange_info.get(symbol1)['price_filter']
                        min_notional = self.exchange_info.get(symbol1)['min_notional']
                        amount = TriangleArbitrage.cut2_float(
                            min(self.calc_exchange_amount(bid_1, ask_2, bid_3),
                                0.015,
                                self.balance['BTC']['free']),
                            lot_size)
                        print amount, self.calc_exchange_amount(bid_1, ask_2, bid_3)

                        order_price = round(bid_1[0][0], price_filter) - 0.1
                        usdt_amount = order_price * amount

                        if amount < 1.0 / (10 ** lot_size):
                            logger.info('BTC数量低于 %s, 本单取消' % (1.0 / (10 ** lot_size)))
                            continue

                        if usdt_amount < min_notional:
                            logger.info('USDT数量小于 %s, 本单取消' % min_notional)
                            continue

                        # p1卖出
                        logger.info('卖出:%s, 价格:%s' % (symbol1, order_price))
                        try:
                            order = self.binanceClient.order_limit_sell(symbol=symbol1, quantity=amount,
                                                                        price=order_price)
                        except Exception as e:
                            logger.error(u'卖出错误: %s' % e)
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('sell orderId: %s, state: %s' % (order_id, order['status']))
                        field_cash_amount = 0
                        field_amount = 0
                        if order['status'] == 'NEW' or order['status'] == 'PARTIALLY_FILLED':
                            logger.info('撤消未完成委托')
                            try:
                                cancel_r = self.binanceClient.cancel_order(symbol=symbol1, orderId=order_id)
                                logger.info('撤销成功')
                                print cancel_r
                            except Exception as e:
                                logger.error(u'撤销错误: %s' % e)

                        logger.info('更新成交量')
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'CANCELED' or current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))

                        # p2买入
                        logger.info('买入: %s, 价格: %s' % (symbol2, ask_2[0][0]))
                        lot_size = self.exchange_info.get(symbol2)['lot_size']
                        price_filter = self.exchange_info.get(symbol2)['price_filter']
                        min_notional = self.exchange_info.get(symbol2)['min_notional']
                        usdt_amount = field_cash_amount
                        format_str = '{:.%sf}' % price_filter
                        order_price = format_str.format(ask_2[0][0] * (1 + 0.005))
                        # order_price = round(ask_2[0][0] * (1 + 0.005), price_filter)
                        amount = TriangleArbitrage.cut2_float(usdt_amount / (ask_2[0][0] * (1 + 0.005)), lot_size)

                        if usdt_amount < min_notional:
                            logger.info('USDT买入数量低于: %s, 本单取消' % min_notional)
                            continue

                        if amount < 10 ** (-1 * lot_size):
                            logger.info('买入数量低于 %s, 本单取消' % (10 ** (-1 * lot_size)))
                            continue
                        try:
                            order = self.binanceClient.order_limit_buy(symbol=symbol2, quantity=amount,
                                                                       price=order_price)
                        except Exception as e:
                            logger.error(u'%s买入错误: %s, 开始回滚' % (symbol2, e))
                            # 回滚
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('buy orderId: %s, state: %s' % (order_id, order['status']))
                        logger.info('更新成交量')
                        field_cash_amount = 0
                        field_amount = 0
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))

                        # p3卖出
                        logger.info('卖出: %s, price: %s' % (symbol3, bid_3[0][0]))
                        lot_size = self.exchange_info.get(symbol3)['lot_size']
                        price_filter = self.exchange_info.get(symbol3)['price_filter']
                        min_notional = self.exchange_info.get(symbol3)['min_notional']
                        amount = TriangleArbitrage.cut2_float(field_amount, lot_size)
                        format_str = '{:.%sf}' % price_filter
                        order_price = format_str.format(bid_3[0][0] * (1 - 0.005))

                        if amount < 10 ** (-1 * lot_size):
                            logger.info('卖出数量低于 %s, 本单取消' % (10 ** (-1 * lot_size)))
                            continue
                        try:
                            order = self.binanceClient.order_limit_sell(symbol=symbol3, quantity=amount,
                                                                        price=order_price)
                        except Exception as e:
                            logger.error(u'%s卖出错误: %s' % (symbol3, e))
                            time.sleep(3)
                            continue
                        print order
                        order_id = order['orderId']
                        logger.info('sell orderId: %s, state: %s' % (order_id, order['status']))
                        logger.info('更新成交量')
                        field_cash_amount = 0
                        field_amount = 0
                        start_timestamp = time.time()
                        while True:
                            if len(self.bn_execution_report) == 0:
                                continue
                            current_status = self.bn_execution_report[-1]['X']
                            if current_status == 'FILLED':
                                for report in self.bn_execution_report:
                                    field_amount += float(report['l'])
                                    field_cash_amount += float(report['l']) * float(report['L'])
                                field_cash_amount = float('%.8f' % field_cash_amount)
                                self.bn_execution_report = []
                                break
                            if time.time() - start_timestamp > 60:
                                break
                        if len(self.bn_execution_report) > 0:
                            logger.info('Binance 订单状态异常，程序终止')
                            break
                        if field_amount == 0:
                            logger.info('未完成任何委托')
                            continue
                        logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                            float(field_amount), float(field_cash_amount)))

                        self.update_account_info()
                        flag = True
                    break
            if flag:
                break


if __name__ == '__main__':
    arbitrage = TriangleArbitrage()
    arbitrage.update_account_info()
    arbitrage.get_exchange_info()
    # arbitrage.test_binance_trade()
    arbitrage.go()
