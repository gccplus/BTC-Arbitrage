# -*- coding: utf-8 -*-
from OKEXService import OkexFutureClient
from HuobiService import HuobiSpot
import logging
import sys
import time
import re
import requests

"""
期期套利
平台：OKEX
"""


class TermArbitrage:
    # 正表示期货价格高于现货
    profit_rate = [0.0035, 0.004, 0.005]
    slippage = 0.0002
    close__thd = 0

    # 风险控制
    margin_thd = 100
    risk_thd = 0.5
    lever_rate = 20

    margin_coefficient = [0.3, 0.8, 1]

    symbol = 'btc_usdt'
    short_contract_type = 'this_week'
    long_contract_type = 'quarter'

    # 每次最大合约交易量
    max_contract_exchange_amount = 15

    # 1 开多 2 开空 3 平多 4 平空
    debug_type = 0

    def __init__(self):
        key_dict = {}
        # 读取配置文件
        with open('config', 'r') as f:
            for line in f.readlines():
                splited = line.split('=')
                if len(splited) == 2:
                    key_dict[splited[0].strip()] = splited[1].strip()

        self.client = OkexFutureClient(key_dict['OKEX_ACCESS_KEY'], key_dict['OKEX_SECRET_KEY'])
        self.spot_client = HuobiSpot(key_dict['HUOBI_ACCESS_KEY'], key_dict['HUOBI_SECRET_KEY'])

        # config logging
        self.logger = logging.getLogger("Future")

        # 指定logger输出格式
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.FileHandler("term2.log")
        file_handler.setFormatter(formatter)

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter

        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)

        # 空头合约数量
        self.short_bear_amount = 0
        self.long_bear_amount = 0

        # 多头合约数量
        self.short_bull_amount = 0
        self.long_bull_amount = 0

        # 保证金
        self.keep_deposit = 0
        self.risk_rate = 0
        self.future_rights = 0
        self.profit_real = 0
        self.profit_unreal = 0
        self.bond = 0
        # 指数
        self.future_index = 0

        # 等待交割
        self.wait_for_delivery = False

    def update_future_position(self):
        self.logger.info('全仓用户持仓查询')
        try:
            info = self.client.position(self.symbol, self.short_contract_type)
        except Exception as e:
            self.logger.error('全仓用户持仓查询异常: %s' % e)
        else:
            # print info
            if info['result']:
                holding = info['holding']
                if len(holding) > 0:
                    # hold是数组
                    self.short_bull_amount = holding[0]['buy_amount']
                    self.short_bear_amount = holding[0]['sell_amount']
        try:
            info = self.client.position(self.symbol, self.long_contract_type)
        except Exception as e:
            self.logger.error('全仓用户持仓查询异常: %s' % e)
        else:
            # print info
            if info['result']:
                holding = info['holding']
                if len(holding) > 0:
                    # hold是数组
                    self.long_bull_amount = holding[0]['buy_amount']
                    self.long_bear_amount = holding[0]['sell_amount']
        self.logger.info('近期多仓: %s  近期空仓: %s' % (self.short_bull_amount, self.short_bear_amount))
        self.logger.info('远期多仓: %s  远期空仓: %s' % (self.long_bull_amount, self.long_bear_amount))

    def update_future_account(self, vertify=False):
        times = 0
        while times < 10:
            old_bond = self.bond
            self.logger.info('第%s次获取全仓账户信息' % (times + 1))
            try:
                future_info = self.client.userinfo()
            except Exception as e:
                self.logger.error('获取Future全仓账户信息异常: %s' % e)
            else:
                # print future_info
                if future_info['result']:
                    btc_info = future_info['info']['btc']
                    print btc_info
                    self.keep_deposit = btc_info['keep_deposit']
                    self.risk_rate = btc_info['risk_rate']
                    self.future_rights = btc_info['account_rights']
                    self.profit_real = btc_info['profit_real']
                    self.profit_unreal = btc_info['profit_unreal']
                    self.bond = self.keep_deposit
                    self.logger.info('bond: %s\trights: %s' % (self.bond, self.future_rights))
            if not vertify or abs(self.bond - old_bond) > 1e-9:
                break
            times += 1
            if times == 9:
                time.sleep(3)
        if times == 10:
            self.logger.info('账户信息未更新')

    def test_order(self):
        orderid = '684493875909632'
        try:
            order_info = self.client.order_info(self.symbol, self.long_contract_type, 0,
                                                orderid, 1, 5)
        except Exception as e:
            self.logger.error('获取订单信息异常: %s, 程序终止' % e)
        else:
            print order_info

    def test_available(self):
        out = open('diff', 'a+')
        i = 0
        out_format = "{}     {:<10.2f}{:<10.2f}{:<10.2f}{:<10.2f}\n"
        while True:
            try:
                short_ticker = self.client.ticker(self.symbol, self.short_contract_type)
            except Exception as e:
                print('获取近期合约行情错误: %s' % e)
                time.sleep(3)
                continue
            try:
                long_ticker = self.client.ticker(self.symbol, self.long_contract_type)
            except Exception as e:
                print('获取远期合约行情错误: %s' % e)
                time.sleep(3)
                continue
            spot_ticker = self.spot_client.get_ticker('btcusdt')
            if spot_ticker['status'] == 'ok':
                pass
            else:
                print 'spot ticker error'
                time.sleep(3)
                continue

            diff1 = short_ticker['ticker']['buy'] - long_ticker['ticker']['sell']
            diff2 = short_ticker['ticker']['sell'] - long_ticker['ticker']['buy']
            diff3 = short_ticker['ticker']['buy'] - spot_ticker['tick']['bid'][0]
            diff4 = short_ticker['ticker']['sell'] - spot_ticker['tick']['ask'][0]
            nowtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            print out_format.format(nowtime, diff1, diff2, diff3, diff4)
            out.write(out_format.format(nowtime, diff1, diff2, diff3, diff4))
            i += 1
            if i % 2 == 0:
                out.flush()
            time.sleep(5)

    def calc_available_contract(self, price):
        available_btc = self.future_rights - self.bond
        if self.risk_rate:
            pass
        return int(available_btc * price / 20) if available_btc > 0 else 0

    def calc_exchange_contract_amount(self, short_depth, long_depth):
        short_amount, long_amount = 0, 0
        for d in short_depth:
            if abs(d[0] - short_depth[0][0]) / short_depth[0][0] <= self.slippage / 2:
                short_amount += d[1]
        for d in long_depth:
            if abs(d[0] - long_depth[0][0]) / long_depth[0][0] <= self.slippage:
                long_amount += d[1]
        return min(int(short_amount), int(long_amount / 2))

    def rollback_order(self, type, price, amount):
        pass

    @staticmethod
    def sms_notify(msg):
        url = 'http://221.228.17.88:8080/sendmsg/send'
        params = {
            'phonenum': '18118999630',
            'msg': msg
        }
        requests.get(url, params=params)

    def go(self):
        while True:
            # 放到前面
            localtime = time.localtime()
            nowtime = time.strftime('%H:%M:%S', localtime)
            weekday = time.strftime("%w", localtime)
            print nowtime, weekday
            if nowtime.startswith('08:30'):
                self.logger.info('短信通知')
                TermArbitrage.sms_notify('rights: %s' % (self.future_rights - self.profit_unreal))
                time.sleep(60)
            if weekday == 5 and re.match(r'^1[0-6].*', nowtime):
                self.logger.info('等待交割')
                self.wait_for_delivery = True

            print('获取深度')
            try:
                short_depth = self.client.depth(self.symbol, self.short_contract_type, 10)
            except Exception as e:
                print('获取近期市场深度错误: %s' % e)
                time.sleep(3)
                continue
            short_bids = short_depth['bids']
            short_asks = short_depth['asks'][::-1]

            try:
                long_depth = self.client.depth(self.symbol, self.long_contract_type, 10)
            except Exception as e:
                print('获取远期市场深度错误: %s' % e)
                time.sleep(3)
                continue
            long_bids = long_depth['bids']
            long_asks = long_depth['asks'][::-1]

            print short_asks[0][0] - long_bids[0][0]
            print short_bids[0][0] - long_asks[0][0]

            price_index = (long_bids[0][0] + short_asks[0][0]) / 2
            open_thd = self.profit_rate * price_index

            if not self.wait_for_delivery and (
                    self.debug_type == 1 or short_asks[0][0] - long_bids[0][0] < -1 * open_thd):
                # 近期开多，远期开空
                contract_amount = min(
                    self.max_contract_exchange_amount,
                    self.calc_available_contract(short_asks[0][0]),
                    self.calc_exchange_contract_amount(short_asks, long_bids)
                )
                if contract_amount == 0:
                    time.sleep(3)
                    continue
                self.logger.info('近期开多: %s张' % contract_amount)
                order_price = float('%.2f' % (short_asks[0][0] * (1 + self.slippage)))
                try:
                    order = self.client.place_order(self.symbol, self.short_contract_type, order_price,
                                                    contract_amount, '1', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    continue
                print order

                orderid = order['order_id']
                try:
                    order_info = self.client.order_info(self.symbol, self.short_contract_type, 0,
                                                        orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s, 程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.client.cancel(self.symbol, self.short_contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                    else:
                        self.logger.info('撤销成功')

                    self.logger.info('更新订单状态')
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中 5:撤单中
                    times = 0
                    while times < 20:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        try:
                            order_info = self.client.order_info(self.symbol, self.short_contract_type,
                                                                0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' % order_info['status'])

                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 19:
                            self.logger.info('撤单处理中...')
                            time.sleep(3)
                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break
                deal_contract_amount = order_info['deal_amount']
                deal_price = order_info['price_avg']
                if deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                self.short_bull_amount += deal_contract_amount
                self.logger.info('short_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))

                self.logger.info('远期开空: %s张' % deal_contract_amount)
                order_price = float('%.2f' % (long_bids[0][0] * (1 - 0.002)))
                try:
                    order = self.client.place_order(self.symbol, self.long_contract_type, order_price,
                                                    deal_contract_amount, '2', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    break
                print order

                orderid = order['order_id']
                # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                deal_contract_amount = 0
                deal_price = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次查询订单状态' % (times + 1))
                    try:
                        order_info = self.client.order_info(self.symbol, self.long_contract_type,
                                                            0, orderid, 1, 5)
                        print order_info
                        order_info = order_info['orders'][0]
                        self.logger.info('订单状态: %s' % order_info['status'])
                        if order_info['status'] == 2:
                            deal_contract_amount = order_info['deal_amount']
                            deal_price = order_info['price_avg']
                            self.long_bear_amount += deal_contract_amount
                            break
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                    times += 1
                    if times == 9:
                        time.sleep(2)
                    if times == 19:
                        time.sleep(15)
                if times == 20:
                    self.logger.error('未知错误，程序终止')
                    break
                self.logger.info('long_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))
                self.update_future_account(True)

            if not self.wait_for_delivery and (self.debug_type == 2 or short_bids[0][0] - long_asks[0][0] > open_thd):
                # 近期开空，远期开多
                contract_amount = min(
                    self.max_contract_exchange_amount,
                    self.calc_available_contract(short_bids[0][0]),
                    self.calc_exchange_contract_amount(short_bids, long_asks)
                )
                if contract_amount == 0:
                    time.sleep(3)
                    continue
                self.logger.info('近期开空: %s张' % contract_amount)
                order_price = float('%.2f' % (short_bids[0][0] * (1 - self.slippage)))
                try:
                    order = self.client.place_order(self.symbol, self.short_contract_type, order_price,
                                                    contract_amount, '2', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    continue
                print order

                orderid = order['order_id']
                try:
                    order_info = self.client.order_info(self.symbol, self.short_contract_type, 0,
                                                        orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s, 程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.client.cancel(self.symbol, self.short_contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                    else:
                        self.logger.info('撤销成功')

                    self.logger.info('更新订单状态')
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    times = 0
                    while times < 20:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        try:
                            order_info = self.client.order_info(self.symbol, self.short_contract_type,
                                                                0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' % order_info['status'])
                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 19:
                            self.logger.info('撤单处理中...')
                            time.sleep(3)
                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break
                print order_info
                deal_contract_amount = order_info['deal_amount']
                deal_price = order_info['price_avg']

                if deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                self.short_bear_amount += deal_contract_amount
                self.logger.info('short_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))

                self.logger.info('远期开多: %s张' % deal_contract_amount)
                order_price = float('%.2f' % (long_asks[0][0] * (1 + 0.002)))
                try:
                    order = self.client.place_order(self.symbol, self.long_contract_type, order_price,
                                                    deal_contract_amount, '1', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    break
                print order

                orderid = order['order_id']
                # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                deal_contract_amount = 0
                deal_price = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次查询订单状态' % (times + 1))
                    try:
                        order_info = self.client.order_info(self.symbol, self.long_contract_type,
                                                            0, orderid, 1, 5)
                        print order_info
                        order_info = order_info['orders'][0]
                        self.logger.info('订单状态: %s' % order_info['status'])
                        if order_info['status'] == 2:
                            deal_contract_amount = order_info['deal_amount']
                            deal_price = order_info['price_avg']
                            self.long_bull_amount += deal_contract_amount
                            break
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                    times += 1

                    if times == 9:
                        time.sleep(1)
                    if times == 10:
                        time.sleep(15)
                if times == 20:
                    self.logger.error('未知错误，程序终止')
                    break
                self.logger.info('long_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))
                self.update_future_account(True)
            if self.short_bull_amount > 0 and (self.debug_type == 3 or short_bids[0][0] - long_asks[0][0] > 0):
                # 近期平多，远期平空
                contract_amount = min(
                    self.max_contract_exchange_amount,
                    self.short_bull_amount,
                    self.calc_exchange_contract_amount(short_bids, long_asks)
                )
                if contract_amount == 0:
                    time.sleep(3)
                    continue
                self.logger.info('近期平多: %s张' % contract_amount)
                order_price = float('%.2f' % (short_bids[0][0] * (1 - self.slippage)))
                try:
                    order = self.client.place_order(self.symbol, self.short_contract_type, order_price,
                                                    contract_amount, '3', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    continue
                print order

                orderid = order['order_id']
                try:
                    order_info = self.client.order_info(self.symbol, self.short_contract_type, 0,
                                                        orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s, 程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.client.cancel(self.symbol, self.short_contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                    else:
                        self.logger.info('撤销成功')

                    self.logger.info('更新订单状态')
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    times = 0
                    while times < 10:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        try:
                            order_info = self.client.order_info(self.symbol, self.short_contract_type,
                                                                0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' % order_info['status'])

                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9 and order_info['status'] == 4:
                            self.logger.info('撤单处理中...')
                            time.sleep(3)
                    if times == 10:
                        self.logger.error('未知错误，程序终止')
                        break
                deal_contract_amount = order_info['deal_amount']
                deal_price = order_info['price_avg']
                if deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                self.short_bull_amount -= deal_contract_amount
                self.logger.info('short_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))

                self.logger.info('远期平空: %s张' % deal_contract_amount)
                order_price = float('%.2f' % (long_asks[0][0] * (1 + 0.002)))
                try:
                    order = self.client.place_order(self.symbol, self.long_contract_type, order_price,
                                                    deal_contract_amount, '4', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    break
                print order

                orderid = order['order_id']
                # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                deal_contract_amount = 0
                deal_price = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次查询订单状态' % (times + 1))
                    try:
                        order_info = self.client.order_info(self.symbol, self.long_contract_type,
                                                            0, orderid, 1, 5)
                        print order_info
                        order_info = order_info['orders'][0]
                        self.logger.info('订单状态: %s' % order_info['status'])
                        if order_info['status'] == 2:
                            deal_contract_amount = order_info['deal_amount']
                            deal_price = order_info['price_avg']
                            self.long_bear_amount -= deal_contract_amount
                            break
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                    times += 1

                    if times == 9:
                        time.sleep(1)
                    if times == 19:
                        time.sleep(15)
                if times == 20:
                    self.logger.error('未知错误，程序终止')
                    break
                self.logger.info('long_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))
                if self.short_bull_amount == 0:
                    time.sleep(15)
                    self.update_future_account()

            if self.short_bear_amount > 0 and (self.debug_type == 4 or short_asks[0][0] - long_bids[0][0] < 0):
                # 近期平空，远期平多
                contract_amount = min(
                    self.max_contract_exchange_amount,
                    self.short_bear_amount,
                    self.calc_exchange_contract_amount(short_asks, long_bids)
                )
                if contract_amount == 0:
                    time.sleep(3)
                    continue
                self.logger.info('近期平空: %s张' % contract_amount)
                order_price = float('%.2f' % (short_bids[0][0] * (1 + self.slippage)))
                try:
                    order = self.client.place_order(self.symbol, self.short_contract_type, order_price,
                                                    contract_amount, '4', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    continue
                print order

                orderid = order['order_id']
                try:
                    order_info = self.client.order_info(self.symbol, self.short_contract_type, 0,
                                                        orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s, 程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.client.cancel(self.symbol, self.short_contract_type, orderid)
                    except Exception as e:
                        self.logger.error('撤销异常: %s' % e)
                    else:
                        self.logger.info('撤销成功')

                    self.logger.info('更新订单状态')
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    times = 0
                    while times < 10:
                        self.logger.info('第%s次查询订单状态' % (times + 1))
                        try:
                            order_info = self.client.order_info(self.symbol, self.short_contract_type,
                                                                0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info('订单状态: %s' % order_info['status'])

                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9 and order_info['status'] == 4:
                            self.logger.info('撤单处理中...')
                            time.sleep(3)
                    if times == 10:
                        self.logger.error('未知错误，程序终止')
                        break
                deal_contract_amount = order_info['deal_amount']
                deal_price = order_info['price_avg']

                if deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                self.short_bear_amount -= deal_contract_amount
                self.logger.info('short_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))

                self.logger.info('远期平多: %s张' % deal_contract_amount)
                order_price = float('%.2f' % (long_asks[0][0] * (1 - 0.002)))
                try:
                    order = self.client.place_order(self.symbol, self.long_contract_type, order_price,
                                                    deal_contract_amount, '3', '0', lever_rate=self.lever_rate)
                except Exception as e:
                    self.logger.error('订单异常: %s' % e)
                    break
                print order

                orderid = order['order_id']
                # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                deal_contract_amount = 0
                deal_price = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次查询订单状态' % (times + 1))
                    try:
                        order_info = self.client.order_info(self.symbol, self.long_contract_type,
                                                            0, orderid, 1, 5)
                        print order_info
                        order_info = order_info['orders'][0]
                        self.logger.info('订单状态: %s' % order_info['status'])
                        if order_info['status'] == 2:
                            deal_contract_amount = order_info['deal_amount']
                            deal_price = order_info['price_avg']
                            self.long_bull_amount -= deal_contract_amount
                            break
                    except Exception as e:
                        self.logger.error('查询订单信息异常: %s' % e)
                    times += 1

                    if times == 9:
                        time.sleep(1)
                    if times == 19:
                        time.sleep(15)
                if times == 20:
                    self.logger.error('未知错误，程序终止')
                    break
                self.logger.info('long_deal_contract: %d\tprice: %s' % (deal_contract_amount, deal_price))

                if self.short_bear_amount == 0:
                    time.sleep(15)
                    self.update_future_account()

            time.sleep(1)


if __name__ == '__main__':
    term = TermArbitrage()
    # term.update_future_account()
    # term.update_future_position()
    # term.test_order()
    # term.go()
    term.test_available()
    # term.judge_trend2()
