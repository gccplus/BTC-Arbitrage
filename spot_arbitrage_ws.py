# -*- coding: utf-8 -*-
from HuobiService import HuobiSpot
from binance.client import Client as BinanceSpot
from binance.websockets import BinanceSocketManager
from huobi_websockets import HuobiSocketManager
import logging
import logging.handlers
import sys
import time, datetime
import re
import xlsxwriter, xlrd
import requests
import yagmail

"""
平台间套利，websockets版本
"""


class ArbitrageStratety:
    # 手续费率
    huobi_fee_rate = 0.0005
    binance_fee_rate = 0.0005
    slippage = 0.0001
    bnb_price = 10.8159
    # 盈利率
    huobi_profit_rate = 0.0008
    binance_profit_rate = 0.0008
    # btc每次最大交易量
    btc_exchange_min = 0.001
    usdt_exchange_min = 10
    # 程序里有3处需要同时更改
    btc_exchange_max = 0.2
    # HUOBI API最大连续超时次数
    huobi_max_timeout = 3

    def __init__(self):
        key_dict = {}
        # 读取配置文件
        with open('config', 'r') as f:
            for line in f.readlines():
                splited = line.split('=')
                if len(splited) == 2:
                    key_dict[splited[0].strip()] = splited[1].strip()

        self.output = open('history', 'a+')
        self.huobiSpot = HuobiSpot(key_dict['HUOBI_ACCESS_KEY2'], key_dict['HUOBI_SECRET_KEY2'])
        self.binanceClient = BinanceSpot(key_dict['BINANCE_ACCESS_KEY'], key_dict['BINANCE_SECRET_KEY'])
        # websocket binance
        bm = BinanceSocketManager(self.binanceClient)
        # bm.start_depth_socket('BTCUSDT', self.on_binance_message)
        bm.start_depth_socket('BTCUSDT', self.on_binance_message, depth=BinanceSocketManager.WEBSOCKET_DEPTH_10)
        bm.start_user_socket(self.on_binance_message)
        bm.start()

        # websocket huobi
        hm = HuobiSocketManager(self.on_huobi_message)
        hm.start()

        self.btc_mat = "BTC :\tfree:{:<20.8f}locked:{:<20.8f}"
        self.usdt_mat = "USDT:\tfree:{:<20.8f}locked:{:<20.8f}"
        self.total_format = "BTC:{:<20.8f}USDT:{:<20.8f}"

        # config logging
        self.logger = logging.getLogger("Robot")
        # logging.basicConfig()

        # 指定logger输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.handlers.TimedRotatingFileHandler('log', when='midnight')
        # 设置日志文件后缀，以当前时间作为日志文件后缀名。
        file_handler.suffix = "%Y-%m-%d"
        # 可以通过setFormatter指定输出格式
        file_handler.setFormatter(formatter)

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值

        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)

        # 用于记录huobi连续响应过长时间的次数，超过两次，就退出
        self.huobi_timeout = 0

        # 收益统计
        self.huobi_usdt_inc = 0
        self.huobi_usdt_dec = 0
        self.binance_usdt_inc = 0
        self.binance_usdt_dec = 0

        self.huobi_usdt_total_change = 0
        self.binance_usdt_total_change = 0

        # 账户余额
        self.huobi_loan_btc = 0
        self.huobi_interest_btc = 0

        self.huobi_loan_usdt = 0
        self.huobi_interest_usdt = 0

        self.huobi_trade_btc = 0
        self.huobi_trade_usdt = 0
        self.huobi_trade_point = 0
        self.binance_trade_btc = 0
        self.binance_trade_usdt = 0

        # 成交量统计
        self.usdt_exchange_amount = 0
        self.btc_exchange_amount = 0

        self.last_deal_time = 0

        # 由于数量小于0.001而未能成交
        # >0 表示需要卖出的
        self.untreated_btc = 0

        self.huobi_depth_asks = None
        self.huobi_depth_bids = None
        self.huobi_depth = None
        self.binance_depth = None
        self.bn_execution_report = []
        self.outbound_account_info = None

        # symbol
        self.huobi_symbols = {}

    def close_socket(self):
        """关闭所有socketn接口"""
        pass

    def update_symbols(self):
        # huobi
        huobi_symbols = self.huobiSpot.get_symbols()
        if huobi_symbols['status'] == 'ok':
            for item in huobi_symbols['data']:
                quoto = item['quote-currency']
                base = item['base-currency']
                symbol = '{}{}'.format(base, quoto)
                self.huobi_symbols.__setitem__(symbol, item)
            print self.huobi_symbols
        else:
            print huobi_symbols
            if 'fail' == huobi_symbols['status']:
                self.logger.error('Huobi get_symbol error: %s' % huobi_symbols['msg'])
            else:
                self.logger.error('Huobi get_symbol error: %s' % huobi_symbols['err-msg'])
        # binance

    @staticmethod
    def sms_notify(msg):
        url = 'http://221.228.17.88:8080/sendmsg/send'
        params = {
            'phonenum': '18118999630',
            'msg': msg
        }
        requests.get(url, params=params)

    def on_huobi_message(self, msg):
        # print msg
        self.huobi_depth = msg
        if msg.has_key('tick'):
            if len(msg['tick']['asks']) > 0:
                self.huobi_depth_asks = msg['tick']['asks']
            if len(msg['tick']['bids']) > 0:
                self.huobi_depth_bids = msg['tick']['bids']

    def on_binance_message(self, msg):
        # print msg
        if msg.has_key('lastUpdateId'):
            self.binance_depth = msg
        elif msg['e'] == 'error':
            # close and restart the socket
            print 'binance ws error'
        elif msg['e'] == 'executionReport':
            self.bn_execution_report.append(msg)
        elif msg['e'] == 'outboundAccountInfo':
            self.outbound_account_info = msg
        elif msg['e'] == 'depthUpdate':
            pass
        else:
            print("message type: {}".format(msg['e']))
            print(msg)

    def update_profit_rate(self):
        self.logger.info('更新盈利率')
        huobi_btc_percent = float('%.2f' % (self.huobi_trade_btc / (self.huobi_trade_btc + self.binance_trade_btc)))
        binance_btc_percent = 1 - huobi_btc_percent

        self.logger.info('Huobi: %s\tBinance:%s' % (huobi_btc_percent, binance_btc_percent))
        if huobi_btc_percent < 0.2:
            self.huobi_profit_rate = 0.0016
        elif huobi_btc_percent < 0.4:
            self.huobi_profit_rate = 0.0012
        elif huobi_btc_percent > 0.8:
            self.binance_profit_rate = 0.0016
        elif huobi_btc_percent > 0.6:
            self.binance_profit_rate = 0.0012
        else:
            self.huobi_profit_rate = 0.0008
            self.binance_profit_rate = 0.0008
        self.logger.info('huobi_profit_rate: %s\t, binance_profit_rate: %s' % (
            self.huobi_profit_rate, self.binance_profit_rate))

    def update_account_info(self):
        # update binance
        self.logger.info('|--------------------------------------------------')
        self.logger.info('|' + '更新账户信息')
        try:
            account_info = self.binanceClient.get_account()
        except Exception as e:
            self.logger.error('Binance get_account error: %s' % e)
        else:
            freezed_btc = 0
            freezed_usdt = 0
            for info in account_info['balances']:
                if info['asset'] == 'BTC':
                    self.binance_trade_btc = float(info['free'])
                    freezed_btc = float(info['locked'])
                elif info['asset'] == 'USDT':
                    self.binance_trade_usdt = float(info['free'])
                    freezed_usdt = float(info['locked'])
            self.logger.info('|' + 'Binance:')
            self.logger.info('|' + self.btc_mat.format(self.binance_trade_btc, freezed_btc))
            self.logger.info('|' + self.usdt_mat.format(self.binance_trade_usdt, freezed_usdt))

            # update huobi
            # 修复了float进位的问题
            json_r = self.huobiSpot.margin_balance('btcusdt')
            if json_r['status'] == 'ok':
                for item in json_r['data'][0]['list']:
                    if item['currency'] == 'btc' and item['type'] == 'trade':
                        self.huobi_trade_btc = ArbitrageStratety.cut2_float(item['balance'], 8)
                    elif item['currency'] == 'btc' and item['type'] == 'frozen':
                        freezed_btc = float(item['balance'])
                    elif item['currency'] == 'usdt' and item['type'] == 'trade':
                        self.huobi_trade_usdt = ArbitrageStratety.cut2_float(item['balance'], 8)
                    elif item['currency'] == 'usdt' and item['type'] == 'frozen':
                        freezed_usdt = float(item['balance'])
                    elif item['currency'] == 'btc' and item['type'] == 'loan':
                        self.huobi_loan_btc = float(item['balance'])
                    elif item['currency'] == 'usdt' and item['type'] == 'loan':
                        self.huobi_loan_usdt = float(item['balance'])
                    elif item['currency'] == 'btc' and item['type'] == 'interest':
                        self.huobi_interest_btc = float(item['balance'])
                    elif item['currency'] == 'usdt' and item['type'] == 'interest':
                        self.huobi_interest_usdt = float(item['balance'])
                self.logger.info('|' + 'Huobi:')
                self.logger.info('|' + self.btc_mat.format(self.huobi_trade_btc, freezed_btc))
                self.logger.info('|' + self.usdt_mat.format(self.huobi_trade_usdt, freezed_usdt))

                self.logger.info('|' + 'Total:')
                self.logger.info('|' + self.total_format.format(self.binance_trade_btc + self.huobi_trade_btc,
                                                                self.binance_trade_usdt + self.huobi_trade_usdt))
                self.logger.info('|' + 'Untreated: %s' % self.untreated_btc)
            else:
                print json_r
                if 'fail' == json_r['status']:
                    self.logger.error('Huobi get_balance error: %s' % json_r['msg'])
                else:
                    self.logger.error('Huobi get_balance error: %s' % json_r['err-msg'])
            # huobi 点卡
            point_info = self.huobiSpot.get_hbpoint()
            if point_info['status'] == 'ok':
                for item in point_info['data']['list']:
                    if item['currency'] == 'hbpoint' and item['type'] == 'trade':
                        self.huobi_trade_point = ArbitrageStratety.cut2_float(item['balance'], 2)
                        break
                self.logger.info('|' + 'Hbpoint: %s' % self.huobi_trade_point)
            else:
                print point_info
                if 'fail' == json_r['status']:
                    self.logger.error('Huobi get_point_balance error: %s' % json_r['msg'])
                else:
                    self.logger.error('Huobi get_point balance error: %s' % json_r['err-msg'])
            self.update_profit_rate()
            self.logger.info('|--------------------------------------------------')

    @staticmethod
    def merge_depth(depth):
        new_depth = []
        for d in depth:
            price = ArbitrageStratety.cut2_float(d[0], 1)
            # price = float(re.match('(\d+\.\d)\d*', '{:.8f}'.format(float(d[0]))).group(1))
            amount = float(d[1])
            if len(new_depth) == 0:
                new_depth.append([price, amount])
            else:
                if new_depth[-1][0] == price:
                    new_depth[-1] = [price, new_depth[-1][1] + amount]
                else:
                    new_depth.append([price, amount])
        return new_depth[:3]

    @staticmethod
    def cut2_float(s, n):
        s = '{:.8f}'.format(float(s))
        pattern = re.compile(r'(\d+\.\d{1,%d})\d*' % n)
        return float(pattern.match(s).group(1))

    def calc_exchange_amount(self, d1, d2):
        amount1, amount2, amount3 = 0, 0, 0
        for d in d1:
            if abs(d[0] - d1[0][0]) / d1[0][0] < self.slippage:
                amount1 += d[1]
        for d in d2:
            if abs(d[0] - d2[0][0]) / d2[0][0] < self.slippage:
                amount2 += d[1]
            elif abs(d[0] - d2[0][0]) / d2[0][0] < self.slippage * 2:
                amount3 += d[1]
        return min(amount1 / 2, amount2, amount3 / 2)

    def go(self):
        time.sleep(5)
        last_huobi_updateid = self.huobi_depth['ts']
        last_binance_updateid = self.binance_depth['lastUpdateId']
        time.sleep(2)
        while True:
            if self.binance_depth['lastUpdateId'] <= last_binance_updateid or \
                    self.huobi_depth['ts'] <= last_huobi_updateid:
                print '等待深度更新'
                time.sleep(0.1)
                continue
            last_binance_updateid = self.binance_depth['lastUpdateId']
            last_huobi_updateid = self.huobi_depth['ts']
            print 'last: %s, %s' % (last_binance_updateid, last_huobi_updateid)

            h_bids = self.huobi_depth_bids[:10]
            h_asks = self.huobi_depth_asks[:10]
            # 合并深度
            b_bids = [(float(i[0]), float(i[1])) for i in self.binance_depth['bids'][:10]]
            b_asks = [(float(i[0]), float(i[1])) for i in self.binance_depth['asks'][:10]]
            print h_bids
            # print h_asks

            # print b_bids
            print b_asks
            # huobi sell
            if h_bids[0][0] * 1.0 / b_asks[0][
                0] > 1 + self.huobi_fee_rate + self.binance_fee_rate + self.huobi_profit_rate:
                self.logger.info('binance买入,huobi卖出')

                self.logger.info(
                    '卖出价:%s, 买入价:%s, 比例:%s' % (h_bids[0][0], b_asks[0][0], h_bids[0][0] * 1.0 / b_asks[0][0]))

                btc_amount = ArbitrageStratety.cut2_float(min(self.calc_exchange_amount(b_asks, h_bids),
                                                              self.btc_exchange_max, self.huobi_trade_btc), 4)

                order_price = float('%.2f' % (b_asks[0][0] * (1 + self.slippage)))
                usdt_amount = float('%.4f' % (btc_amount * order_price))
                if usdt_amount > self.binance_trade_usdt:
                    usdt_amount = self.binance_trade_usdt
                    btc_amount = ArbitrageStratety.cut2_float(('%.4f' % (usdt_amount / order_price)), 4)
                self.logger.info('本次交易量：%s BTC, %s USDT' % (btc_amount, usdt_amount))

                if btc_amount < self.btc_exchange_min:
                    self.logger.info('BTC交易数量不足: %s, 本单取消' % self.btc_exchange_min)
                    time.sleep(1)
                    continue
                if usdt_amount < self.usdt_exchange_min:
                    self.logger.info('USDT交易数量不足: %s, 本单取消' % self.usdt_exchange_min)
                    time.sleep(1)
                    continue

                # 限价买
                self.logger.info('开始限价买入')
                try:
                    buy_order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=btc_amount,
                                                                   price=order_price, newOrderRespType='FULL')
                except Exception as e:
                    self.logger.error(u'Binance买入错误: %s' % e)
                    time.sleep(3)
                    continue
                print buy_order

                buy_order_id = buy_order['orderId']
                self.output.write('\n' + str(buy_order_id))
                self.output.flush()
                self.logger.info('binance buy orderId: %s, state: %s' % (buy_order_id, buy_order['status']))
                field_cash_amount = 0
                field_amount = 0
                if buy_order['status'] == 'NEW' or buy_order['status'] == 'PARTIALLY_FILLED':
                    self.logger.info('撤消未完成委托')
                    try:
                        cancel_r = self.binanceClient.cancel_order(symbol='BTCUSDT', orderId=buy_order_id)
                        self.logger.info('撤销成功')
                        print cancel_r
                    except Exception as e:
                        self.logger.error(u'撤销错误: %s' % e)

                # 4/28 更新
                # 实际中出现FILLED比NEW来的早的情况,程序需要做出调整
                # 暂不调整，观察下次是否再次出现此次问题
                self.logger.info('更新成交量')
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

                if field_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))

                self.binance_trade_btc += field_amount
                self.binance_trade_usdt -= field_cash_amount

                self.binance_usdt_dec = field_cash_amount

                self.binance_usdt_total_change -= self.binance_usdt_dec

                # 市价卖
                self.logger.info('开始市价卖出')
                btc_amount = float('%.4f' % field_amount)

                # 记录总共损失的BTC数目，达到0.001时候，进行补全
                if btc_amount < self.btc_exchange_min:
                    self.logger.info('BTC卖出数量低于 %s', self.btc_exchange_min)
                    self.logger.info('本次交易终止')
                    self.untreated_btc += field_amount
                    # self.rollback_binance_order(buy_order_id)
                    time.sleep(3)
                    continue
                # 买入卖出由于精度不同，会存在一定的偏差，这里进行统计调整
                else:
                    self.untreated_btc += field_amount - btc_amount

                sell_order = self.huobiSpot.send_order(btc_amount, 'margin-api', 'btcusdt', 'sell-market')
                if sell_order['status'] != 'ok':
                    if sell_order['status'] == 'fail':
                        self.logger.error('sell failed : %s' % sell_order['msg'])
                    else:
                        self.logger.error('sell failed : %s' % sell_order['err-msg'])

                    self.logger.info('开始回滚')
                    if self.rollback_binance_order('BUY', order_price, btc_amount):
                        self.logger.info('回滚成功')
                    else:
                        self.logger.info('回滚失败')
                    self.logger.info('终止程序')
                    break
                sell_order_id = sell_order['data']

                self.output.write(':' + str(sell_order_id))
                self.output.flush()
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    sell_order_info = self.huobiSpot.order_info(sell_order_id)
                    print sell_order_info
                    if sell_order_info['status'] == 'ok' and sell_order_info['data']['state'] == 'filled':
                        self.logger.info('huobi sell filled, orderId: %s' % sell_order_id)
                        field_cash_amount = sell_order_info['data']['field-cash-amount']
                        field_amount = sell_order_info['data']['field-amount']
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)

                if times == 20:
                    self.huobi_timeout += 1
                    if self.huobi_timeout == self.huobi_max_timeout:
                        self.logger.info('连续%s次超时，终止程序' % self.huobi_timeout)
                        break
                    else:
                        self.logger.info('连续%s次超时，继续程序' % self.huobi_timeout)
                        if self.huobi_timeout == 1:
                            time.sleep(60)
                        else:
                            time.sleep(600)
                        self.btc_exchange_max /= 2
                        continue
                else:
                    self.huobi_timeout = 0
                    self.btc_exchange_max = 0.2

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))

                self.huobi_trade_btc -= float(field_amount)
                self.huobi_trade_usdt += float(field_cash_amount)
                # 更新交易量统计
                self.usdt_exchange_amount += float(field_cash_amount)
                self.btc_exchange_amount += float(field_amount)

                self.huobi_usdt_inc = float(field_cash_amount)

                self.huobi_usdt_total_change += self.huobi_usdt_inc

                usdt_inc = self.huobi_usdt_inc - self.binance_usdt_dec
                thistime_earnings = usdt_inc
                thistime_earnings_rate = thistime_earnings * 1.0 / float(field_cash_amount)

                total_usdt_earnings = self.huobi_usdt_total_change + self.binance_usdt_total_change

                self.logger.info('本次交易量: %s BTC, 盈利: %s USDT, 盈利率: %s' % (
                    float(field_amount), thistime_earnings, thistime_earnings_rate))
                self.logger.info(
                    '总BTC成交量: %s, 盈利: %s USDT, 盈利率: %s' % (
                        self.btc_exchange_amount, total_usdt_earnings,
                        total_usdt_earnings * 1.0 / self.usdt_exchange_amount))
                self.logger.info('|--------------------------------------------------')
                self.logger.info('|' + ' BTC:\tTOTAL:{:<20.8f}HUOBI:{:<20.8f}BINANCE:{:<20.8f}'.format(
                    (self.huobi_trade_btc + self.binance_trade_btc), self.huobi_trade_btc, self.binance_trade_btc))
                self.logger.info(
                    '|' + 'USDT:\tTOTAL:{:<20.8f}HUOBI:{:<20.8f}BINANCE:{:<20.8f}'.format(
                        (self.huobi_trade_usdt + self.binance_trade_usdt), self.huobi_trade_usdt,
                        self.binance_trade_usdt))
                self.logger.info('|--------------------------------------------------')
                self.update_profit_rate()

                self.last_deal_time = int(time.time())

            # binance sell
            elif b_bids[0][0] * 1.0 / h_asks[0][
                0] > 1 + self.huobi_fee_rate + self.binance_fee_rate + self.binance_profit_rate:
                self.logger.info('binance 卖出, huobi买入')

                self.logger.info(
                    '卖出价:%s, 买入价:%s, 比例:%s' % (b_bids[0][0], h_asks[0][0], b_bids[0][0] / h_asks[0][0]))

                btc_amount = ArbitrageStratety.cut2_float(min(self.calc_exchange_amount(b_bids, h_asks),
                                                              self.binance_trade_btc,
                                                              self.btc_exchange_max), 4)

                order_price = float('%.2f' % (b_bids[0][0] * (1 - self.slippage)))
                usdt_amount = float('%.4f' % (btc_amount * (h_asks[0][0] + 20)))
                if usdt_amount > self.huobi_trade_usdt:
                    usdt_amount = self.huobi_trade_usdt
                    btc_amount = ArbitrageStratety.cut2_float(usdt_amount / (h_asks[0][0] + 20), 4)
                self.logger.info('本次交易量：%s BTC, %s USDT' % (btc_amount, usdt_amount))

                if btc_amount < self.btc_exchange_min:
                    self.logger.info('BTC交易数量不足: %s, 本单取消' % self.btc_exchange_min)
                    time.sleep(1)
                    continue
                if float('%.4f' % (btc_amount * order_price)) < self.usdt_exchange_min:
                    self.logger.info('USDT交易数量不足: %s, 本单取消' % self.usdt_exchange_min)
                    time.sleep(1)
                    continue

                # 限价卖
                self.logger.info('开始限价卖出')
                try:
                    sell_order = self.binanceClient.order_limit_sell(symbol='BTCUSDT', quantity=btc_amount,
                                                                     price=order_price, newOrderRespType='FULL')
                except Exception as e:
                    self.logger.error(u'Binance卖出错误: %s' % e)
                    time.sleep(3)
                    continue
                print sell_order
                sell_order_id = sell_order['orderId']
                self.output.write('\n' + str(sell_order_id))
                self.output.flush()
                self.logger.info('binance sell orderId: %s, state: %s' % (sell_order_id, sell_order['status']))
                field_cash_amount = 0
                field_amount = 0
                if sell_order['status'] == 'NEW' or sell_order['status'] == 'PARTIALLY_FILLED':
                    self.logger.info('撤消未完成委托')
                    try:
                        cancel_r = self.binanceClient.cancel_order(symbol='BTCUSDT', orderId=sell_order_id)
                        self.logger.info('撤销成功')
                        print cancel_r
                    except Exception as e:
                        self.logger.error(u'撤销错误: %s' % e)

                self.logger.info('更新成交量')
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
                if field_amount == 0:
                    self.logger.info('未完成任何委托')
                    continue

                # 更新统计数据
                self.btc_exchange_amount += float(field_amount)
                self.usdt_exchange_amount += float(field_cash_amount)

                # update income
                self.binance_usdt_inc = field_cash_amount

                self.binance_usdt_total_change += self.binance_usdt_inc

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    field_amount, field_cash_amount))
                self.binance_trade_btc -= field_amount
                self.binance_trade_usdt += field_cash_amount

                # 限价买,使买价高于市价，最后会已市价成交
                self.logger.info('开始伪市价（限价）买入')
                buy_price = h_asks[0][0] + 20
                btc_amount = float('%.4f' % field_amount)

                if btc_amount < self.btc_exchange_min:
                    self.logger.error('BTC交易数量低于 %s' % self.btc_exchange_min)
                    self.logger.info('本次交易终止')
                    self.untreated_btc -= field_amount
                    time.sleep(3)
                    continue
                else:
                    self.untreated_btc -= field_amount - btc_amount

                buy_r = self.huobiSpot.send_order(btc_amount, 'margin-api', 'btcusdt', 'buy-limit', buy_price)
                print buy_r

                if buy_r['status'] != 'ok':
                    if buy_r['status'] == 'fail':
                        self.logger.error('buy failed : %s' % buy_r['msg'])
                    else:
                        self.logger.error('buy failed : %s' % buy_r['err-msg'])
                    self.logger.info('开始回滚')
                    if self.rollback_binance_order('SELL', order_price, btc_amount):
                        self.logger.info('回滚成功')
                    else:
                        self.logger.info('回滚失败')
                    self.logger.info('终止程序')
                    break

                buy_order_id = buy_r['data']
                self.output.write(':' + str(buy_order_id))
                self.output.flush()
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    buy_order_result = self.huobiSpot.order_info(buy_order_id)
                    print buy_order_result
                    if buy_order_result['status'] == 'ok' and buy_order_result['data']['state'] == 'filled':
                        self.logger.info('huobi buy filled, orderId: %s' % buy_order_id)
                        field_amount = float('%.8f' % float(buy_order_result['data']['field-amount']))
                        field_cash_amount = float('%.8f' % float(buy_order_result['data']['field-cash-amount']))
                        break
                    times += 1
                    if times == 19:
                        time.sleep(15)

                if times == 20:
                    self.huobi_timeout += 1
                    if self.huobi_timeout == self.huobi_max_timeout:
                        self.logger.info('连续%s次超时，终止程序' % self.huobi_timeout)
                        break
                    else:
                        self.logger.info('连续%s次超时，继续程序' % self.huobi_timeout)
                        if self.huobi_timeout == 1:
                            time.sleep(60)
                        else:
                            time.sleep(600)
                        #
                        self.btc_exchange_max /= 2
                        continue
                else:
                    self.huobi_timeout = 0
                    self.btc_exchange_max = 0.2

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    field_amount, field_cash_amount))
                # update income
                self.huobi_usdt_dec = field_cash_amount

                self.huobi_usdt_total_change -= self.huobi_usdt_dec

                self.huobi_trade_btc += field_amount
                self.huobi_trade_usdt -= field_cash_amount
                # total
                usdt_inc = self.binance_usdt_inc - self.huobi_usdt_dec
                thistime_earnings = usdt_inc
                thistime_earnings_rate = thistime_earnings * 1.0 / field_cash_amount

                # total_btc_earnings = self.huobi_btc_total_change + self.binance_btc_total_change
                total_usdt_earnings = self.huobi_usdt_total_change + self.binance_usdt_total_change

                self.logger.info('本次交易量: %s BTC, 盈利: %s USDT, 盈利率: %s' % (
                    float(field_amount), thistime_earnings, thistime_earnings_rate))
                self.logger.info(
                    '总BTC成交量: %s, 盈利: %s USDT, 盈利率: %s' % (
                        self.btc_exchange_amount, total_usdt_earnings,
                        total_usdt_earnings * 1.0 / self.usdt_exchange_amount))
                self.logger.info('|--------------------------------------------------')
                self.logger.info('|' + ' BTC:\tTOTAL:{:<20.8f}HUOBI:{:<20.8f}BINANCE:{:<20.8f}'.format(
                    (self.huobi_trade_btc + self.binance_trade_btc), self.huobi_trade_btc,
                    self.binance_trade_btc))
                self.logger.info(
                    '|' + 'USDT:\tTOTAL:{:<20.8f}HUOBI:{:<20.8f}BINANCE:{:<20.8f}'.format(
                        (self.huobi_trade_usdt + self.binance_trade_usdt), self.huobi_trade_usdt,
                        self.binance_trade_usdt))
                self.logger.info('|--------------------------------------------------')
                self.update_profit_rate()

                self.last_deal_time = int(time.time())

            nowtime = time.strftime('%H:%M:%S', time.localtime(time.time()))
            if nowtime.startswith('08:30'):
                self.order_statistics()

                self.usdt_exchange_amount = 0
                self.btc_exchange_amount = 0
                self.huobi_usdt_total_change = 0
                self.binance_usdt_total_change = 0
                time.sleep(60)

            # 每5分钟没有交易就更新账户信息
            if self.last_deal_time > 0 and int(time.time()) - self.last_deal_time > 300:
                orderid = 0
                if self.untreated_btc > 0.001:
                    sell_amount = float('%.4f' % self.untreated_btc)
                    self.logger.info('平衡账户资产,Huobi卖出: %s BTC' % sell_amount)
                    sell_r = self.huobiSpot.send_order(sell_amount, 'api', 'btcusdt', 'sell-market')
                    if sell_r['status'] != 'ok':
                        if sell_r['status'] == 'fail':
                            self.logger.error('sell failed : %s' % sell_r['msg'])
                        else:
                            self.logger.error('sell failed : %s' % sell_r['err-msg'])
                        break
                    else:
                        orderid = sell_r['data']
                        self.untreated_btc -= sell_amount
                elif self.untreated_btc < -0.001:
                    buy_price = h_asks[0][0] + 20
                    buy_amount = float('%.4f' % self.untreated_btc)
                    self.logger.info('平衡账户资产,Huobi买入: %s BTC' % buy_amount)
                    buy_r = self.huobiSpot.send_order(-1 * buy_amount, 'api', 'btcusdt', 'buy-limit', buy_price)
                    if buy_r['status'] != 'ok':
                        if buy_r['status'] == 'fail':
                            self.logger.error('sell failed : %s' % buy_r['msg'])
                        else:
                            self.logger.error('sell failed : %s' % buy_r['err-msg'])
                        break
                    else:
                        orderid = buy_r['data']
                        self.untreated_btc -= buy_amount
                print orderid
                if orderid:
                    times = 0
                    while times < 20:
                        self.logger.info('第%s次确认订单信息' % (times + 1))
                        order_result = self.huobiSpot.order_info(orderid)
                        print order_result
                        if order_result['status'] == 'ok' and order_result['data']['state'] == 'filled':
                            self.logger.info('order filled, orderId: %s' % orderid)
                            field_amount = float('%.8f' % float(order_result['data']['field-amount']))
                            field_cash_amount = float('%.8f' % float(order_result['data']['field-cash-amount']))
                            if 'buy' in order_result['data']['type']:
                                self.huobi_trade_btc += field_amount
                                self.huobi_trade_usdt -= field_cash_amount
                            else:
                                self.huobi_trade_btc -= field_amount
                                self.huobi_trade_usdt += field_cash_amount
                            break
                        times += 1

                        if times == 9:
                            time.sleep(10)
                        if times == 19:
                            time.sleep(300)

                    if times == 20:
                        self.logger.error('未知错误，程序终止')
                        break

                total_btc_amount_before = self.binance_trade_btc + self.huobi_trade_btc
                self.logger.info('before: binance：%s\thuobi: %s' % (self.binance_trade_btc, self.huobi_trade_btc))
                self.update_account_info()
                self.logger.info('after : binance：%s\thuobi: %s' % (self.binance_trade_btc, self.huobi_trade_btc))
                total_btc_amount_after = self.binance_trade_btc + self.huobi_trade_btc
                if abs(total_btc_amount_after - total_btc_amount_before) > 0.001:
                    self.logger.info('账户BTC总量发生异常，程序终止')
                    break
                self.last_deal_time = 0

    def rollback_binance_order(self, side, price, amount):
        if side == 'BUY':
            try:
                order = self.binanceClient.order_limit_sell(symbol='BTCUSDT', quantity=amount,
                                                            price=price - 20, newOrderRespType='FULL')
                print order
            except Exception as e:
                self.logger.error(u'Binance卖出错误: %s' % e)
        else:
            try:
                order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=amount,
                                                           price=price + 20, newOrderRespType='FULL')
                print order
            except Exception as e:
                self.logger.error(u'Binance买入错误: %s' % e)

        start_timestamp = time.time()
        while True:
            if len(self.bn_execution_report) == 0:
                continue
            current_status = self.bn_execution_report[-1]['X']
            if current_status == 'FILLED':
                self.bn_execution_report = []
                break
            if time.time() - start_timestamp > 120:
                break
        if len(self.bn_execution_report) > 0:
            self.logger.info('Binance 订单状态异常')
            return False
        return True

    # HUOBI API 提供的只能获取100条，此函数为扩展，提取500条
    def get_huobi_orders(self):
        result = []
        last_order_id = None
        for i in range(5):
            orders_list = self.huobiSpot.orders_list('btcusdt', '', _from=last_order_id, size=100)
            if orders_list['status'] != 'ok':
                print ('获取火币历史委托错误')
                print orders_list
                return []
            else:
                if i == 0:
                    for item in orders_list['data']:
                        result.append(item)
                else:
                    for item in orders_list['data'][1:]:
                        result.append(item)
                last_order_id = orders_list['data'][-1]['id']
        return result

    def order_statistics(self, start=None, end=None):
        huobi_order_list = []
        binance_order_list = []
        # Huobi
        for order in self.get_huobi_orders():
            dict = {
                'id': order['id'],
                'state': order['state'].upper(),
                'amount': float('%.8f' % float(order['amount'])),
                'field-cash-amount': float('%.4f' % float(order['field-cash-amount'])),
                'field-amount': float('%.8f' % float(order['field-amount'])),
                'created-at': order['created-at'],
                'finished-at': order['finished-at'],
                'canceled-at': order['canceled-at'],
                'type': u'LIMIT' if 'limit' in order['type'] else u'MARKET',
                'side': u'BUY' if 'buy' in order['type'] else u'SELL',
            }
            # print dict
            if dict['finished-at'] == 0:
                continue
            if dict['field-amount'] > 0:
                dict['price'] = float('%.2f' % (dict['field-cash-amount'] / dict['field-amount']))
            huobi_order_list.append(dict)
        print('获取币安历史委托数据')
        binance_orders = self.binanceClient.get_all_orders(symbol='BTCUSDT')
        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT')
        # print binance_trades
        for order in binance_orders:
            dict = {
                'id': order['orderId'],
                'state': order['status'].upper(),
                'amount': float('%.8f' % float(order['origQty'])),
                'created-at': order['time'],
                'finished-at': '',
                'canceled-at': '',
                'type': order['type'].upper(),
                'side': order['side'].upper(),
                'commission': 0,
                'field-amount': 0,
                'field-cash-amount': 0
            }
            binance_order_list.append(dict)
        for iter in binance_order_list:
            id = iter['id']
            for trade in binance_trades:
                if trade['orderId'] == id:
                    iter['commission'] += float('%.8f' % float(trade['commission']))
                    iter['field-amount'] += float('%.8f' % float(trade['qty']))
                    iter['price'] = float('%.2f' % float(trade['price']))
                    iter['field-cash-amount'] += float('%.8f' % (float(trade['qty']) * iter['price']))
        huobi_order_list = sorted(huobi_order_list, key=lambda x: x['created-at'], reverse=True)
        binance_order_list = sorted(binance_order_list, key=lambda x: x['created-at'], reverse=True)

        yestoday = datetime.date.today() + datetime.timedelta(days=-1)
        timearray = time.strptime(str(yestoday) + ' 8:30:00', "%Y-%m-%d %H:%M:%S")
        timestamp = int(round(time.mktime(timearray)) * 1000)
        print timestamp
        workbook = xlsxwriter.Workbook('income_%s.xlsx' % (datetime.datetime.now().strftime('%Y_%m_%d')))
        worksheet = workbook.add_worksheet(u'成功')
        worksheet2 = workbook.add_worksheet(u'失败')
        worksheet3 = workbook.add_worksheet(u'总计')
        date_format_str = 'yy/mm/dd/ hh:mm:ss'
        binance_common_format = workbook.add_format({'align': 'left', 'font_name': 'Consolas'})
        binance_date_format = workbook.add_format({'num_format': date_format_str,
                                                   'align': 'left', 'font_name': 'Consolas'})

        huobi_common_format = workbook.add_format({'align': 'left', 'font_name': 'Consolas', 'bg_color': 'yellow'})
        huobi_date_format = workbook.add_format({'num_format': date_format_str,
                                                 'align': 'left', 'font_name': 'Consolas', 'bg_color': 'yellow'})
        merged_format = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'font_name': 'Consolas'})
        row_1 = 0
        row_2 = 0
        row_3 = 0
        col = 0
        header = [u'委托时间', u'方向', u'类型', u'价格', u'委托数量', u'成交数量', u'成交金额', u'状态',
                  u'盈利(USDT)', u'盈利率']

        total_usdt_earnings = 0
        total_huobi_usdt_trade = 0
        total_binance_commission = 0
        total_btc_exchange = 0
        i = 0
        for h in header:
            worksheet.write(row_1, col + i, h)
            worksheet2.write(row_2, col + i, h)
            i += 1
        row_1 += 1
        row_2 += 1
        with open('history', 'r') as f:
            for line in f.readlines():
                if len(line) < 5:
                    continue
                splited = line.strip().split(':')
                huobi_id = 0
                if len(splited) == 2:
                    binance_id = int(splited[0])
                    huobi_id = int(splited[1])
                elif len(splited) == 1:
                    binance_id = int(splited[0])
                else:
                    continue
                # print binance_id, huobi_id
                if binance_id > 0 and huobi_id > 0:
                    binance_order = None
                    huobi_order = None
                    for order in binance_order_list:
                        if order['created-at'] < timestamp:
                            continue
                        if order['id'] == binance_id:
                            binance_order = order
                            break
                    for order in huobi_order_list:
                        if order['created-at'] < timestamp:
                            continue
                        if order['id'] == huobi_id:
                            huobi_order = order
                            break
                    if not binance_order or not huobi_order:
                        continue
                    # print binance_id, huobi_id
                    total_huobi_usdt_trade += float('%.8f' % float(huobi_order['field-cash-amount']))
                    total_binance_commission += float('%.8f' % float(binance_order['commission']))
                    total_btc_exchange += float('%.8f' % float(binance_order['field-amount']))

                    order = huobi_order
                    worksheet.write(row_1, col, datetime.datetime.fromtimestamp(
                        float(str(order['created-at'])[0:-3] + '.' + str(order['created-at'])[-3:0])),
                                    huobi_date_format)
                    worksheet.write(row_1, col + 1, order['side'], huobi_common_format)
                    worksheet.write(row_1, col + 2, order['type'], huobi_common_format)
                    worksheet.write(row_1, col + 3, '%.2f' % float(order['price']), huobi_common_format)
                    worksheet.write(row_1, col + 4, '%.4f' % float(order['amount']), huobi_common_format)
                    worksheet.write(row_1, col + 5, '%.8f' % float(order['field-amount']), huobi_common_format)
                    worksheet.write(row_1, col + 6, '%.8f' % float(order['field-cash-amount']), huobi_common_format)
                    worksheet.write(row_1, col + 7, order['state'], huobi_common_format)
                    row_1 += 1

                    order = binance_order
                    worksheet.write_datetime(row_1, col, datetime.datetime.fromtimestamp(
                        float(str(order['created-at'])[0:-3] + '.' + str(order['created-at'])[-3:0])),
                                             binance_date_format)
                    worksheet.write(row_1, col + 1, order['side'], binance_common_format)
                    worksheet.write(row_1, col + 2, order['type'], binance_common_format)
                    worksheet.write(row_1, col + 3, '%.2f' % float(order['price']), binance_common_format)
                    worksheet.write(row_1, col + 4, '%.4f' % float(order['amount']), binance_common_format)
                    worksheet.write(row_1, col + 5, '%.8f' % float(order['field-amount']), binance_common_format)
                    worksheet.write(row_1, col + 6, '%.8f' % float(order['field-cash-amount']),
                                    binance_common_format)
                    worksheet.write(row_1, col + 7, order['state'], binance_common_format)

                    earnings = abs(float(huobi_order['field-cash-amount']) - float(binance_order['field-cash-amount']))
                    earning_rate = earnings / binance_order['field-cash-amount']

                    total_usdt_earnings += earnings

                    worksheet.merge_range(row_1 - 1, 8, row_1, 8, '%.8f' % earnings, merged_format)
                    worksheet.merge_range(row_1 - 1, 9, row_1, 9, '%.8f' % earning_rate, merged_format)
                    row_1 += 1
                else:
                    for order in binance_order_list:
                        # print order['created-at']
                        if order['created-at'] < timestamp:
                            continue
                        if order['id'] == binance_id and order['field-amount'] > 0:
                            print order
                        if order['id'] == binance_id and 'CANCELED' in order['state']:
                            worksheet2.write_datetime(row_2, col, datetime.datetime.fromtimestamp(
                                float(str(order['created-at'])[0:-3] + '.' + str(order['created-at'])[-3:0])),
                                                      binance_date_format)
                            worksheet2.write(row_2, col + 1, order['side'], binance_common_format)
                            worksheet2.write(row_2, col + 2, order['type'], binance_common_format)
                            worksheet2.write(row_2, col + 3, 0, binance_common_format)
                            worksheet2.write(row_2, col + 4, '%.4f' % float(order['amount']), binance_common_format)
                            worksheet2.write(row_2, col + 5, 0, binance_common_format)
                            worksheet2.write(row_2, col + 6, 0, binance_common_format)
                            worksheet2.write(row_2, col + 7, order['state'], binance_common_format)
                            row_2 += 1
                            break
            total_huobi_commission = total_huobi_usdt_trade * 0.002
            total_binance_commission = total_binance_commission * 10.8159
            # print total_huobi_commission, total_binance_commission
            self.update_account_info()
            total_btc = self.huobi_trade_btc + self.binance_trade_btc
            total_usdt = self.huobi_trade_usdt + self.binance_trade_usdt
            now_time = datetime.datetime.now()
            yes_time = now_time + datetime.timedelta(days=-1)

            try:
                data = xlrd.open_workbook('income_%s.xlsx' % (yes_time.strftime('%Y_%m_%d')))
                table = data.sheet_by_index(2)
            except Exception as e:
                self.logger.info(e)
                nrows = 0
                ncols = 0
            else:
                nrows = table.nrows
                ncols = table.ncols

            top = workbook.add_format(
                {'border': 1, 'align': 'center', 'bg_color': 'cccccc', 'font_size': 13, 'bold': True})
            blank = workbook.add_format({'border': 1})

            for i in range(nrows):
                for j in range(ncols):
                    cell_value = table.cell_value(i, j, )
                    if i == 0:
                        cell_format = top
                    else:
                        cell_format = blank
                    worksheet3.write(i, j, cell_value, cell_format)
                row_3 += 1
            if row_3 == 0:
                header = [u'日期', u'BTC总量', u'BTC成交量', u'USDT总量', u'USDT盈亏', u'HUOBI点卡总量', u'HUOBI手续费', u'BINANCE手续费']
                i = 0
                for h in header:
                    worksheet3.write(row_3, i, h)
                    i += 1
                row_3 += 1
            worksheet3.write(row_3, 0, now_time.strftime('%Y-%m-%d'))
            worksheet3.write(row_3, 1, total_btc)
            worksheet3.write(row_3, 2, total_btc_exchange)
            worksheet3.write(row_3, 3, total_usdt)
            worksheet3.write(row_3, 4, total_usdt_earnings)
            worksheet3.write(row_3, 5, self.huobi_trade_point)
            worksheet3.write(row_3, 6, total_huobi_commission)
            worksheet3.write(row_3, 7, total_binance_commission)
        workbook.close()
        # 发送邮件
        self.logger.info('邮件通知')
        subject = u'总成交量(%s)_收益情况(%s)' % (total_btc_exchange, time.strftime('%Y-%m-%d', time.localtime(time.time())))
        body = 'sending with attachment'
        attachment = 'income_%s.xlsx' % datetime.datetime.now().strftime('%Y_%m_%d')

        yag = yagmail.SMTP(user='skfornotify@gmail.com', password='skp8eL9%', host='smtp.gmail.com')
        yag.send(to='18118999630@189.cn', subject=subject, contents=[body, attachment])
        self.logger.info('邮件通知完成')


if __name__ == '__main__':
    strategy = ArbitrageStratety()
    strategy.update_account_info()
    # strategy.update_symbols()
    # strategy.order_statistics()
    strategy.go()
    # ArbitrageStratety.sms_notify('对冲程序已终止')
