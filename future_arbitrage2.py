# -*- coding: utf-8 -*-
from OKEXService import OkexFutureClient
from HuobiService import HuobiSpot
import logging
import sys
import time
import numpy as np
import re
from threading import Thread
import websocket
import zlib
import json

"""
期现套利
现货：火币
期货：OKEX
"""


class TermArbitrage:
    # 正表示期货价格高于现货
    profit_rate = 0.05
    slippage = 0.0002
    # open_positive_rate = 40
    close__thd = 0

    # open_negative_thd = -40
    # close_thd = 0

    # 风险控制
    margin_thd = 200
    risk_thd = 0.5

    margin_coefficient = 0.6

    symbol = 'btc_usdt'
    contract_type = 'quarter'

    # 每次最大合约交易量
    max_contract_exchange_amount = 5

    # 1 开多 2 开空 3 平多 4 平空
    debug_type = 0

    status_dict = ['']

    def __init__(self):
        key_dict = {}
        # 读取配置文件
        with open('config', 'r') as f:
            for line in f.readlines():
                splited = line.split('=')
                if len(splited) == 2:
                    key_dict[splited[0].strip()] = splited[1].strip()

        self.huobi_client = HuobiSpot(
            key_dict['HUOBI_ACCESS_KEY3'], key_dict['HUOBI_SECRET_KEY3'])
        self.future_client = OkexFutureClient(
            key_dict['OKEX_ACCESS_KEY'], key_dict['OKEX_SECRET_KEY'])

        # 空头合约数量
        self.bear_amount = 0

        # 多头合约数量
        self.bull_amount = 0

        # 保证金
        self.keep_deposit = 0
        self.risk_rate = 0
        self.future_rights = 0
        self.profit_real = 0
        self.profit_unreal = 0
        self.bond = 0
        # 指数
        self.future_index = 0

        self.spot_free_btc = 0
        self.spot_free_usdt = 0
        self.spot_freezed_btc = 0
        self.spot_freezed_usdt = 0

        self.total_btc = 0
        self.total_usdt = 0

        # 统计收益

        # config logging
        self.logger = logging.getLogger("Future")

        # 指定logger输出格式
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.FileHandler("term.log")
        file_handler.setFormatter(formatter)

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter

        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)

        # 平均成交差价
        self.avg_price_diff = 0

        # 2018/4/18
        self.spot_kline = []
        self.future_kline = []

    def update_future_position(self):
        self.logger.info('全仓用户持仓查询')
        try:
            info = self.future_client.position(self.symbol, self.contract_type)
        except Exception as e:
            self.logger.error('全仓用户持仓查询异常: %s' % e)
        else:
            # print info
            if info['result']:
                holding = info['holding']
                if len(holding) > 0:
                    # hold是数组
                    self.bull_amount = holding[0]['buy_amount']
                    self.bear_amount = holding[0]['sell_amount']
                    if self.bull_amount == 0 and self.bear_amount == 0:
                        self.logger.info('用户未持仓')
                        self.total_btc = self.future_rights + self.spot_free_btc
                        self.total_usdt = self.spot_free_usdt
                        self.logger.info('total:')
                        self.logger.info('BTC: %s\tUSDT: %s' % (self.total_btc, self.total_usdt))
                    else:
                        self.logger.info('多仓: %s\t空仓: %s' %
                                         (self.bull_amount, self.bear_amount))
            else:
                self.logger.info('postion_4fix result error')

    def update_spot_account(self):
        self.logger.info('获取SPOT账户信息')
        r = self.huobi_client.margin_balance('btcusdt')
        # print r
        if r['status'] == 'ok':
            for item in r['data'][0]['list']:
                if item['currency'] == 'btc' and item['type'] == 'trade':
                    self.spot_free_btc = TermArbitrage.cut2_float(item['balance'], 8)
                elif item['currency'] == 'btc' and item['type'] == 'frozen':
                    freezed_btc = float(item['balance'])
                elif item['currency'] == 'usdt' and item['type'] == 'trade':
                    self.spot_free_usdt = TermArbitrage.cut2_float(item['balance'], 8)
                elif item['currency'] == 'usdt' and item['type'] == 'frozen':
                    freezed_usdt = float(item['balance'])
                elif item['currency'] == 'btc' and item['type'] == 'loan':
                    spot_loan_btc = float(item['balance'])
                elif item['currency'] == 'usdt' and item['type'] == 'loan':
                    spot_loan_usdt = float(item['balance'])
                elif item['currency'] == 'btc' and item['type'] == 'interest':
                    spot_interest_btc = float(item['balance'])
                elif item['currency'] == 'usdt' and item['type'] == 'interest':
                    spot_interest_usdt = float(item['balance'])
            self.logger.info('spot_btc:%s\tspot_usdt:%s' %
                             (self.spot_free_btc, self.spot_free_usdt))
        else:
            if 'fail' == r['status']:
                self.logger.error('Huobi get_balance error: %s' % r['msg'])
            else:
                self.logger.error('Huobi get_balance error: %s' % r['err-msg'])

    def update_future_account(self, vertify=False):
        times = 0
        while times < 10:
            old_bond = self.bond
            self.logger.info('第%s次获取全仓账户信息' % (times + 1))
            try:
                future_info = self.future_client.userinfo()
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
                    self.bond = self.keep_deposit + self.profit_unreal / 10
                    self.logger.info('bond:%s\trights:%s' % (self.bond, self.future_rights))
            if not vertify or abs(self.bond - old_bond) > 1e-9:
                break
            times += 1
            if times == 9:
                time.sleep(3)
        if times == 10:
            self.logger.info('账户信息未更新')

    @staticmethod
    def cut2_float(s, n):
        if isinstance(s, float):
            s = '{:.8f}'.format(s)
        pattern = re.compile(r'(\d+\.\d{1,%d})\d*' % n)
        return float(pattern.match(s).group(1))

    def calc_available_contract(self, price):
        available_btc = self.future_rights * self.margin_coefficient - self.bond
        return int(available_btc * price / 10) if available_btc > 0 else 0

    def judge_trend2(self):
        nowtime = time.strftime('%H:%M:%S', time.localtime(time.time()))
        second = int(nowtime.split(':')[2])
        if 30 < second < 40:
            self.future_kline = []
            self.spot_kline = []
            # .logger.info('get spot kline')
            spot_kline = self.huobi_client.get_kline('btcusdt', '1min', 120)
            if spot_kline['status'] == 'ok':
                # print spot_kline
                for item in spot_kline['data']:
                    self.spot_kline.append({
                        'ts': item['id'],
                        'open': item['open'],
                        'close': item['close'],
                        'high': item['high'],
                        'low': item['low'],
                        'avg': float('%.2f' % (item['vol'] / item['amount'])) if item['amount'] > 0
                        else (item['high'] + item['low']) / 2
                    })
            else:
                self.logger.info('get spot kline error')

            # self.logger.info('get future kline')
            try:
                future_kline = self.future_client.kline(self.symbol, '1min', self.contract_type)
            except Exception as e:
                self.logger.error('get future kline error: %s' % e)
            else:
                for item in future_kline[::-1][:120]:
                    self.future_kline.append({
                        'ts': item[0] / 1000,
                        'open': item[1],
                        'close': item[4],
                        'high': item[2],
                        'low': item[3],
                        'avg': float('%.2f' % (100 * item[5] / item[6])) if item[6] > 0 else (item[2] + item[3]) / 2
                    })
            max_diff1 = -1 << 31
            max_diff2 = -1 << 31
            min_diff1 = (1 << 31) - 1
            min_diff2 = (1 << 31) - 1
            # print self.spot_kline
            # print self.future_kline
            if len(self.spot_kline) == 120 and len(self.future_kline) == 120 and self.future_kline[0]['ts'] == \
                    self.spot_kline[0]['ts']:
                cur_diff = self.future_kline[0]['avg'] - self.spot_kline[0]['avg']
                for i in xrange(1, 60):
                    diff = self.future_kline[i]['avg'] - self.spot_kline[i]['avg']
                    if diff > max_diff1:
                        max_diff1 = diff
                    elif diff < min_diff1:
                        min_diff1 = diff
                    max_diff2 = max_diff1
                    min_diff2 = min_diff1
                for i in xrange(60, 120):
                    diff = self.future_kline[i]['avg'] - self.spot_kline[i]['avg']
                    if diff > max_diff2:
                        max_diff2 = diff
                    elif diff < min_diff2:
                        min_diff2 = diff
                print cur_diff, max_diff1, max_diff2
                print cur_diff, min_diff1, min_diff2

                if cur_diff < 0:
                    if cur_diff - min_diff1 > 10 or cur_diff - min_diff2 > 15:
                        # 下降
                        return 1
                    elif cur_diff - max_diff1 < -10 or cur_diff - max_diff2 < -15:
                        # 上升
                        return 2
                else:
                    if cur_diff - max_diff1 < -10 or cur_diff - max_diff2 < -15:
                        # 下降
                        return 3
                    elif cur_diff - min_diff1 > 10 or cur_diff - min_diff2 > 15:
                        # 上升
                        return 4
        return 0

    def calc_exchange_contract_amount(self, spot_depth, future_depth):
        spot_amount, future_amount = 0, 0
        for d in spot_depth:
            if abs(d[0] - spot_depth[0][0]) / spot_depth[0][0] <= self.slippage / 2:
                spot_amount += d[1]
        for d in future_depth:
            if abs(d[0] - future_depth[0][0]) / future_depth[0][0] <= self.slippage:
                future_amount += d[1]
        return min(int(spot_amount * spot_depth[0][0] / 200), int(future_amount / 2))

    def go(self):
        while True:
            # print('获取深度')
            try:
                future_depth = self.future_client.depth(
                    self.symbol, self.contract_type, 10)
            except Exception as e:
                self.logger.error('获取期货市场深度错误: %s' % e)
                time.sleep(3)
                continue
            future_bids = future_depth['bids']
            future_asks = future_depth['asks'][::-1]

            spot_depth = self.huobi_client.get_depth('btcusdt', 'step5')
            # print spot_depth
            if spot_depth['status'] == 'ok':
                spot_bids = spot_depth['tick']['bids']
                spot_asks = spot_depth['tick']['asks']
            else:
                time.sleep(3)
                self.logger.error('获取现货市场深度错误')
                continue

            price_index = (future_bids[0][0] + spot_asks[0][0]) / 2
            diff_thd = self.profit_rate * price_index
            d1 = future_bids[0][0] - spot_asks[0][0]
            d1 = float('%.2f' % d1)

            trend = self.judge_trend2()
            print d1, trend
            if abs(d1) > self.margin_thd:
                self.logger.info('期现差价超过阈值, 程序终止')
                break
            # 期货价格低于现货，且差价开始降低, 期货开多
            if trend == 1 and spot_bids[0][0] - future_asks[0][0] > diff_thd:
                available_bull_amount = self.calc_available_contract(future_asks[0][0])
                if available_bull_amount == 0:
                    # self.logger.info('可开合约数量不足')
                    time.sleep(3)
                    continue
                self.logger.info('可开合约数量: %s' % available_bull_amount)
                self.logger.info('期货开多,现货卖出')
                self.logger.info('期货价格: %s,现货价格: %s,差价为: %s' %
                                 (future_asks[0][0], spot_bids[0][0], future_asks[0][0] - spot_bids[0][0]))
                self.logger.info(
                    'future_bond:%s\tspot_btc:%s\tspot_usdt:%s' % (self.bond, self.spot_free_btc, self.spot_free_usdt))

                future_contract_amount = min(self.max_contract_exchange_amount,
                                             available_bull_amount,
                                             self.calc_exchange_contract_amount(spot_bids, future_asks),
                                             int(self.spot_free_btc * (spot_bids[0][0] - 20) / 100))
                if future_contract_amount == 0:
                    continue
                self.logger.info('期货开多:%s 张' % future_contract_amount)
                # 限价购买期货合约
                price = float('%.2f' % (future_asks[0][0] * (1 + self.slippage)))
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '1', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
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
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
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
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info(
                                '订单状态: %s' % order_info['status'])
                            # 订单状态还有待确认
                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9:
                            time.sleep(3)
                        if times == 10:
                            time.sleep(10)
                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break
                print order_info

                future_deal_contract_amount = order_info['deal_amount']

                if future_deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue

                future_deal_price = order_info['price_avg']
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10.0 / future_deal_price))

                self.bull_amount += future_deal_contract_amount

                self.logger.info(
                    'future_deal_contract:%d\tbtc:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_price
                    ))
                # 市价卖出现货
                self.logger.info('现货卖出')
                spot_limited_price = spot_bids[0][0] - 20
                spot_btc_amount = float(
                    '%.4f' % (100 * future_deal_contract_amount / spot_limited_price))

                spot_order = self.huobi_client.send_order(
                    spot_btc_amount, 'margin-api', 'btcusdt', 'sell-limit', spot_limited_price)
                print spot_order

                if spot_order['status'] != 'ok':
                    if spot_order['status'] == 'fail':
                        self.logger.error('spot sell failed : %s' % spot_order['msg'])
                    else:
                        self.logger.error('spot sell failed : %s' % spot_order['err-msg'])
                    self.logger.info('开始回滚')
                    self.logger.info('终止程序')
                    break

                orderid = spot_order['data']
                field_cash_amount = 0
                field_amount = 0

                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    order_info = self.huobi_client.order_info(orderid)
                    print order_info
                    if order_info['status'] == 'ok' and order_info['data']['state'] == 'filled':
                        self.logger.info('spot sell filled, orderId: %s' % orderid)
                        field_amount = float('%.8f' % float(order_info['data']['field-amount']))
                        field_cash_amount = float('%.8f' % float(order_info['data']['field-cash-amount']))
                        field_price = float('%.2f' % (field_cash_amount / field_amount))
                        price_diff = future_deal_price - field_price

                        self.avg_price_diff = ((self.bull_amount - future_deal_contract_amount) * self.avg_price_diff +
                                               future_deal_contract_amount * price_diff) / self.bull_amount
                        self.spot_free_btc -= field_amount
                        self.spot_free_usdt += field_cash_amount
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)

                if times == 20:
                    self.logger.info('现货卖出错误, 终止程序')
                    break

                self.logger.info('spot_field_amount:%.8f\tspot_field_cash_amount:%.8f' % (
                    field_amount, field_cash_amount))

                self.logger.info('avg_price_diff: %.2f' % self.avg_price_diff)
                self.update_future_account(True)

                # self.update_account_info()
            # 期货高于现货，且差价开始降低, 期货开空
            elif trend == 3 and future_bids[0][0] - spot_asks[0][0] > diff_thd:
                available_bear_amount = self.calc_available_contract(future_bids[0][0])
                if available_bear_amount == 0:
                    # self.logger.info('可开合约数量不足')
                    time.sleep(3)
                    continue
                self.logger.info('可开合约数量为: %s' % available_bear_amount)
                self.logger.info('期货开空，现货买入')
                self.logger.info('期货价格: %s,现货价格: %s, 差价: %s' % (
                    future_bids[0][0], spot_asks[0][0], future_bids[0][0] - spot_asks[0][0]))
                self.logger.info(
                    'future_bond:%s\tspot_btc:%s\tspot_usdt:%s' % (self.bond, self.spot_free_btc, self.spot_free_usdt))
                future_contract_amount = min(self.calc_exchange_contract_amount(spot_asks, future_bids),
                                             self.max_contract_exchange_amount,
                                             available_bear_amount,
                                             int(self.spot_free_usdt / 100))

                if future_contract_amount == 0:
                    continue
                # 限价购买期货合约
                self.logger.info('期货开空:%s' % future_contract_amount)
                price = float('%.2f' % (future_bids[0][0] * (1 - self.slippage)))
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '2', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('获取订单信息异常: %s,程序终止' % e)
                    break
                print order_info

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
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
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info(
                                '订单状态: %s' % order_info['status'])
                            # 订单状态还有待确认
                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9:
                            time.sleep(3)
                        if times == 19:
                            time.sleep(10)
                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']

                if future_deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue

                future_deal_price = order_info['price_avg']
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10.0 / future_deal_price))

                self.bear_amount += future_deal_contract_amount

                self.logger.info(
                    'future_deal:contract:%d\tbtc:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_price
                    ))
                # 市价买入现货
                spot_usdt_amount = 100 * future_deal_contract_amount
                self.logger.info('现货买入 %sUSDT' % spot_usdt_amount)
                spot_order = self.huobi_client.send_order(
                    spot_usdt_amount, 'margin-api', 'btcusdt', 'buy-market')
                if spot_order['status'] != 'ok':
                    if spot_order['status'] == 'fail':
                        self.logger.error('spot buy failed : %s' % spot_order['msg'])
                    else:
                        self.logger.error('spot buy failed : %s' % spot_order['err-msg'])
                    # TODO
                    self.logger.info('开始回滚')
                    self.logger.info('终止程序')
                    break
                orderid = spot_order['data']
                field_cash_amount = 0
                field_amount = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    order_info = self.huobi_client.order_info(orderid)
                    print order_info
                    if order_info['status'] == 'ok' and order_info['data']['state'] == 'filled':
                        self.logger.info('spot buy filled, orderId: %s' % orderid)
                        field_amount = float('%.8f' % float(order_info['data']['field-amount']))
                        field_cash_amount = float('%.8f' % float(order_info['data']['field-cash-amount']))
                        field_price = float('%.2f' % (field_cash_amount / field_amount))
                        price_diff = future_deal_price - field_price

                        self.avg_price_diff = ((self.bear_amount - future_deal_contract_amount) * self.avg_price_diff +
                                               future_deal_contract_amount * price_diff) / self.bear_amount
                        self.spot_free_btc += field_amount
                        self.spot_free_usdt -= field_cash_amount
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)
                if times == 20:
                    self.logger.info('现货买入错误, 终止程序')
                    break

                self.logger.info('spot_field_amount:%.8f\tspot_field_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))
                self.logger.info('avg_price_diff: %.2f' % self.avg_price_diff)
                self.update_future_account(True)
            # 平空
            if self.bear_amount > 0 and \
                    (self.debug_type == 4
                     or self.risk_rate < self.risk_thd
                     or future_asks[0][0] - spot_bids[0][0] < 0
                     or (trend == 4 and self.avg_price_diff - future_asks[0][0] + spot_bids[0][0] > diff_thd)):

                self.logger.info('期货平空，现货卖出')
                self.logger.info('期货价格: %s,现货价格: %s,差价: %s' % (
                    future_asks[0][0], spot_bids[0][0], future_asks[0][0] - spot_bids[0][0]))
                self.logger.info('当前持空仓: %s' % self.bear_amount)
                self.logger.info(
                    'future_bond:%s\tspot_btc:%s\tspot_usdt:%s' % (self.bond, self.spot_free_btc, self.spot_free_usdt))
                future_contract_amount = min(self.max_contract_exchange_amount,
                                             self.bear_amount,
                                             self.calc_exchange_contract_amount(spot_bids, future_asks),
                                             int(self.spot_free_btc * (float(spot_asks[0][0]) - 20) / 100))

                if future_contract_amount == 0:
                    continue
                self.logger.info('期货平空: %s' % future_contract_amount)

                price = float('%.2f' % (future_asks[0][0] * (1 + self.slippage)))
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '4', '0')
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error('查询订单信息异常:%s, 程序终止' % e)
                    break

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
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
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info(
                                '订单状态: %s' % order_info['status'])
                            # 订单状态还有待确认
                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9:
                            time.sleep(3)
                        if times == 19:
                            time.sleep(10)

                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                future_deal_price = order_info['price_avg']
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10.0 / future_deal_price))

                self.bear_amount -= future_deal_contract_amount

                self.logger.info(
                    'future_deal:contract:%d\tbtc:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_price
                    ))

                spot_limited_price = spot_bids[0][0] - 20
                spot_btc_amount = float(
                    '%.4f' % (100 * future_deal_contract_amount / spot_limited_price))
                self.logger.info('现货卖出: %sUSDT' % (100 * future_deal_contract_amount))

                spot_order = self.huobi_client.send_order(
                    spot_btc_amount, 'margin-api', 'btcusdt', 'sell-limit', spot_limited_price)
                print spot_order

                if spot_order['status'] != 'ok':
                    if spot_order['status'] == 'fail':
                        self.logger.error('spot sell failed : %s' % spot_order['msg'])
                    else:
                        self.logger.error('spot sell failed : %s' % spot_order['err-msg'])
                    self.logger.info('开始回滚')
                    self.logger.info('终止程序')
                    break

                orderid = spot_order['data']
                field_amount = 0
                field_cash_amount = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    order_info = self.huobi_client.order_info(orderid)
                    print order_info
                    if order_info['status'] == 'ok' and order_info['data']['state'] == 'filled':
                        self.logger.info('spot sell filled, orderId: %s' % orderid)
                        field_amount = float('%.8f' % float(order_info['data']['field-amount']))
                        field_cash_amount = float('%.8f' % float(order_info['data']['field-cash-amount']))

                        self.spot_free_btc -= field_amount
                        self.spot_free_usdt += field_cash_amount
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)

                if times == 20:
                    self.logger.info('现货卖出错误, 终止程序')
                    break

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    field_amount, field_cash_amount))

                # 计算当前收益
                if self.bear_amount == 0:
                    time.sleep(15)
                    self.update_spot_account()
                    self.update_future_account()
                    self.logger.info('total_btc: %s\tfuture_rights: %s\tspot_btc: %s' %
                                     (self.future_rights + self.spot_free_btc, self.future_rights, self.spot_free_btc))
            # 平多
            if self.bull_amount > 0 and \
                    (self.debug_type == 3
                     or self.risk_rate < self.risk_thd
                     or future_bids[0][0] - spot_asks[0][0] > 0
                     or (trend == 3 and self.avg_price_diff - future_bids[0][0] + spot_asks[0][0] < -1 * diff_thd)):
                self.logger.info('期货平多，现货买入')
                self.logger.info('期货价格: %s,现货价格 %s, 差价: %s' % (
                    future_bids[0][0], spot_asks[0][0], future_bids[0][0] - spot_asks[0][0]))
                self.logger.info('当前持多仓: %s' % self.bull_amount)
                self.logger.info(
                    'future_bond:%s\tspot_btc:%s\tspot_usdt:%s' % (self.bond, self.spot_free_btc, self.spot_free_usdt))
                future_contract_amount = min(self.max_contract_exchange_amount,
                                             self.bull_amount,
                                             self.calc_exchange_contract_amount(spot_asks, future_bids),
                                             int(self.spot_free_usdt / 100))

                if future_contract_amount == 0:
                    continue
                self.logger.info('期货平多: %s' % future_contract_amount)
                # 限价购买期货合约
                price = float('%.2f' % (future_bids[0][0] * (1 - self.slippage)))
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  future_contract_amount, '3', 0)
                except Exception as e:
                    self.logger.error('Future订单异常: %s' % e)
                    continue
                print future_order

                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0, orderid, 1, 5)
                except Exception as e:
                    self.logger.error('查询订单信息异常: %s,程序终止' % e)
                    break

                order_info = order_info['orders'][0]
                self.logger.info('order status: %s' % order_info['status'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
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
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            print order_info
                            order_info = order_info['orders'][0]
                            self.logger.info(
                                '订单状态: %s' % order_info['status'])
                            # 订单状态还有待确认
                            if order_info['status'] == 2 or order_info['status'] == -1:
                                break
                        except Exception as e:
                            self.logger.error('查询订单信息异常: %s' % e)
                        times += 1

                        if times == 9:
                            time.sleep(3)
                        if times == 19:
                            time.sleep(10)

                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break

                print order_info

                future_deal_contract_amount = order_info['deal_amount']
                # 未完成任何委托
                if future_deal_contract_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue
                future_deal_price = order_info['price_avg']
                # btc精度是8
                future_deal_btc_amount = float(
                    '%.8f' % (future_deal_contract_amount * 10.0 / future_deal_price))

                self.bull_amount -= future_deal_contract_amount

                self.logger.info(
                    'future_deal:contract:%d\tbtc:%s\tprice:%s' % (
                        future_deal_contract_amount, future_deal_btc_amount, future_deal_price
                    ))
                # 现货买入
                spot_usdt_amount = 100 * future_deal_contract_amount
                self.logger.info('现货买入: %sUSDT' % spot_usdt_amount)

                spot_order = self.huobi_client.send_order(spot_usdt_amount, 'margin-api', 'btcusdt', 'buy-market')
                if spot_order['status'] != 'ok':
                    if spot_order['status'] == 'fail':
                        self.logger.error('spot buy failed : %s' % spot_order['msg'])
                    else:
                        self.logger.error('spot buy failed : %s' % spot_order['err-msg'])
                    self.logger.info('开始回滚')
                    self.logger.info('终止程序')
                    break
                orderid = spot_order['data']
                field_cash_amount = 0
                field_amount = 0
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    order_info = self.huobi_client.order_info(orderid)
                    print order_info
                    if order_info['status'] == 'ok' and order_info['data']['state'] == 'filled':
                        self.logger.info(
                            'huobi buy filled, orderId: %s' % orderid)
                        field_amount = float('%.8f' % float(order_info['data']['field-amount']))
                        field_cash_amount = float('%.8f' % float(order_info['data']['field-cash-amount']))

                        self.spot_free_btc += field_amount
                        self.spot_free_usdt -= field_cash_amount
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)

                if times == 20:
                    self.logger.info('现货买入错误, 终止程序')
                    break

                self.logger.info('spot_field_amount:%.8f\tspot_field_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))

                # self.update_future_account()
                # 计算当前收益
                if self.bull_amount == 0:
                    time.sleep(15)
                    self.update_spot_account()
                    self.update_future_account()
                    self.logger.info('total_btc: %s\tfuture_rights: %s\tspot_btc: %s' %
                                     (self.future_rights + self.spot_free_btc, self.future_rights, self.spot_free_btc))

            time.sleep(1)


if __name__ == '__main__':
    term = TermArbitrage()
    term.update_spot_account()
    term.update_future_account()
    term.update_future_position()

    term.go()
