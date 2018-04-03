# -*- coding: utf-8 -*-
from HuobiService import HuobiSpot
from binance.client import Client as BinanceSpot
import logging
import logging.handlers
import sys
import time
import re
import numpy as np
import xlsxwriter
import requests
import datetime


class ArbitrageStratety:
    # 手续费率
    huobi_fee_rate = 0.0012
    binance_fee_rate = 0.0005
    bnb_price = 10.8159
    # 盈利率
    huobi_profit_rate = 0.0003
    binance_profit_rate = 0.0003
    # btc每次最大交易量
    btc_exchange_min = 0.001
    usdt_exchange_min = 10
    # 程序里有3处需要同时更改
    btc_exchange_max = 0.065
    # HUOBI API最大连续超时次数
    huobi_max_timeout = 3
    # 深度参数阈值
    STD_THD = 5
    SUM_THD = 0.5

    def __init__(self):
        key_dict = {}
        # 读取配置文件
        with open('config', 'r') as f:
            for line in f.readlines():
                splited = line.split('=')
                if len(splited) == 2:
                    key_dict[splited[0].strip()] = splited[1].strip()

        self.output = open('history', 'a+')
        self.huobiSpot = HuobiSpot(key_dict['HUOBI_ACCESS_KEY'], key_dict['HUOBI_SECRET_KEY'])
        self.binanceClient = BinanceSpot(key_dict['BINANCE_ACCESS_KEY'], key_dict['BINANCE_SECRET_KEY'])

        self.btc_mat = "BTC :\tfree:{:<20.8f}locked:{:<20.8f}"
        self.usdt_mat = "USDT:\tfree:{:<20.8f}locked:{:<20.8f}"
        self.total_format = "BTC:{:<20.8f}USDT:{:<20.8f}"

        # config logging
        self.logger = logging.getLogger("Robot")

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

        self.huobi_fees = 0
        self.binance_fees = 0

        # 账户余额
        self.huobi_trade_btc = 0
        self.huobi_trade_usdt = 0
        self.binance_trade_btc = 0
        self.binance_trade_usdt = 0

        # 成交量统计
        self.usdt_exchange_amount = 0
        self.btc_exchange_amount = 0

        self.last_deal_time = 0

        # 由于数量小于0.001而未能成交
        # >0 表示需要卖出的
        self.untreated_btc = 0

    @staticmethod
    def sms_notify(msg):
        url = 'http://221.228.17.88:8080/sendmsg/send'
        params = {
            'phonenum': '18118999630',
            'msg': msg
        }
        requests.get(url, params=params)

    def update_profit_rate(self):
        self.logger.info('更新盈利率')
        huobi_btc_percent = float('%.2f' % (self.huobi_trade_btc / (self.huobi_trade_btc + self.binance_trade_btc)))
        binance_btc_percent = 1 - huobi_btc_percent

        self.logger.info('Huobi: %s\tBinance:%s' % (huobi_btc_percent, binance_btc_percent))
        if huobi_btc_percent < 0.1:
            self.huobi_profit_rate = 0.001
        elif huobi_btc_percent < 0.2:
            self.huobi_profit_rate = 0.0007
        elif huobi_btc_percent > 0.9:
            self.binance_profit_rate = 0.001
        elif huobi_btc_percent > 0.8:
            self.binance_profit_rate = 0.0007
        else:
            self.huobi_profit_rate = 0.0003
            self.binance_profit_rate = 0.0003
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
            json_r = self.huobiSpot.get_balance()
            if json_r['status'] == 'ok':
                for item in json_r['data']['list']:
                    if item['currency'] == 'btc' and item['type'] == 'trade':
                        self.huobi_trade_btc = float(item['balance'])
                    elif item['currency'] == 'btc' and item['type'] == 'frozen':
                        freezed_btc = float(item['balance'])
                    elif item['currency'] == 'usdt' and item['type'] == 'trade':
                        self.huobi_trade_usdt = float(item['balance'])
                    elif item['currency'] == 'usdt' and item['type'] == 'frozen':
                        freezed_usdt = float(item['balance'])
                self.logger.info('|' + 'Huobi:')
                self.logger.info('|' + self.btc_mat.format(self.huobi_trade_btc, freezed_btc))
                self.logger.info('|' + self.usdt_mat.format(self.huobi_trade_usdt, freezed_usdt))

                self.logger.info('|' + 'Total:')
                self.logger.info('|' + self.total_format.format(self.binance_trade_btc + self.huobi_trade_btc,
                                                                self.binance_trade_usdt + self.huobi_trade_usdt))
                self.logger.info('|' + 'Untreated: %s' % self.untreated_btc)
                self.update_profit_rate()
            else:
                print json_r
                if 'fail' == json_r['status']:
                    self.logger.error('Huobi get_balance error: %s' % json_r['msg'])
                else:
                    self.logger.error('Huobi get_balance error: %s' % json_r['err-msg'])
            self.logger.info('|--------------------------------------------------')

    @staticmethod
    def merge_depth(depth):
        new_depth = []
        for d in depth:
            price = float(re.match('(\d+\.\d)\d*', '{:.8f}'.format(float(d[0]))).group(1))
            amount = float(d[1])
            if len(new_depth) == 0:
                new_depth.append([price, amount])
            else:
                if new_depth[-1][0] == price:
                    new_depth[-1] = [price, new_depth[-1][1] + amount]
                else:
                    new_depth.append([price, amount])
        return new_depth[:5]

    def go(self):
        while True:
            # get depth info
            print '获取深度信息'
            h_depth = self.huobiSpot.get_depth('btcusdt', 'step5')
            if h_depth['status'] == 'ok':
                h_bids = h_depth['tick']['bids']
                h_asks = h_depth['tick']['asks']
            else:
                time.sleep(3)
                continue

            try:
                b_depth = self.binanceClient.get_order_book(symbol='BTCUSDT')
            except Exception as e:
                self.logger.error(u'获取Binance市场深度错误: %s' % e)
                time.sleep(3)
                continue

            # 需要合并深度
            b_bids = ArbitrageStratety.merge_depth(b_depth['bids'])
            b_asks = ArbitrageStratety.merge_depth(b_depth['asks'])
            print b_bids

            # huobi sell
            if h_bids[0][0] * 1.0 / float(
                    b_asks[0][0]) > 1 + self.huobi_fee_rate + self.binance_fee_rate + self.huobi_profit_rate:
                self.logger.info('binance买入,huobi卖出,')
                print h_bids
                h_sum = np.sum(h_bids[:3], axis=0)
                h_std = np.std(h_bids[:3], axis=0)
                h_avg = np.mean(h_bids[:3], axis=0)

                a = [(float(i[0]), float(i[1])) for i in b_asks]
                b_sum = np.sum(a[:3], axis=0)
                b_std = np.std(a[:3], axis=0)
                b_avg = np.mean(a[:3], axis=0)
                print a
                self.logger.info('ASKS:\tsum:%10.4f\tstd:%10.4f\tavg:%10.4f' % (h_sum[1], h_std[0], h_avg[0]))
                self.logger.info('BIDS:\tsum:%10.4f\tstd:%10.4f\tavg:%10.4f' % (b_sum[1], b_std[0], b_avg[0]))

                self.logger.info(
                    '卖出价:%s, 买入价:%s, 比例:%s' % (h_bids[0][0], float(b_asks[0][0]), h_bids[0][0] * 1.0 / float(
                        b_asks[0][0])))

                if h_std[0] > self.STD_THD or h_sum[1] < self.SUM_THD:
                    self.logger.info('标准差过大，本单取消')
                    time.sleep(0.1)
                    continue

                btc_amount = float(
                    '%.4f' % min(h_bids[0][1], float(b_asks[0][1]), self.btc_exchange_max))

                # Binance btc-amount 精度是6位，需要截取前4位，不能产生进位
                if btc_amount > float(b_asks[0][1]):
                    btc_amount = float(re.match('(\d+\.\d{4})\d*', '{:.8f}'.format(float(b_asks[0][1]))).group(1))

                if btc_amount > self.huobi_trade_btc:
                    btc_amount = float(
                        re.match('(\d+\.\d{4})\d*', '{:.8f}'.format(self.huobi_trade_btc)).group(1))

                order_price = float(b_asks[0][0]) + 0.1
                usdt_amount = float('%.4f' % (btc_amount * order_price))
                self.logger.info('本次交易量：%s BTC, %s USDT' % (btc_amount, usdt_amount))

                if btc_amount < self.btc_exchange_min:
                    self.logger.info('BTC交易数量不足: %s, 本单取消' % self.btc_exchange_min)
                    time.sleep(1)
                    continue
                if usdt_amount < self.usdt_exchange_min:
                    self.logger.info('USDT交易数量不足: %s, 本单取消' % self.usdt_exchange_min)
                    time.sleep(1)
                    continue
                if usdt_amount > self.binance_trade_usdt - 5:
                    self.logger.info('Binance USDT 数量: %s, 不足：%s, 本单取消' % (self.binance_trade_usdt, usdt_amount))
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
                field_fees = 0
                if buy_order['status'] == 'NEW' or buy_order['status'] == 'PARTIALLY_FILLED':
                    self.logger.info('撤消未完成委托')
                    try:
                        cancel_r = self.binanceClient.cancel_order(symbol='BTCUSDT', orderId=buy_order_id)
                        print cancel_r
                    except Exception as e:
                        self.logger.error(u'撤销错误: %s' % e)
                        times = 0
                        while times < 10:
                            self.logger.info(u'第%s次查询Binance订单状态' % times)
                            try:
                                order = self.binanceClient.get_order(symbol='BTCUSDT', orderId=buy_order_id)
                                buy_order['status'] = order['status']
                                self.logger.info(u'当前订单状态为: %s', order['status'])
                                # 撤销失败，状态必定为FILLED?
                                if buy_order['status'] == 'FILLED':
                                    self.logger.info('计算本次成交量')
                                    try:
                                        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT')
                                    except Exception as e:
                                        self.logger.error('Binance get_my_trades error' % e)
                                        times += 1
                                        continue
                                    else:
                                        print binance_trades
                                        for trade in binance_trades:
                                            if trade['orderId'] == buy_order_id:
                                                field_fees += float('%.8f' % float(trade['commission']))
                                                field_amount += float('%.8f' % float(trade['qty']))
                                                price = float('%.2f' % float(trade['price']))
                                                field_cash_amount += float('%.8f' % (float(trade['qty']) * price))
                                        break
                            except Exception as e:
                                self.logger.error(u'get order or get trades error: %s' % e)
                            times += 1

                        if times == 10:
                            self.logger.info('未知错误,程序终止')
                            break
                    # 有可能会出现撤销成功，但是撤销完成的过程中，又完成了部分委托,需要更新实际成交量
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新本次成交量')
                        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT')
                        print binance_trades
                        for trade in binance_trades:
                            if trade['orderId'] == buy_order_id:
                                field_fees += float('%.8f' % float(trade['commission']))
                                field_amount += float('%.8f' % float(trade['qty']))
                                price = float('%.2f' % float(trade['price']))
                                field_cash_amount += float('%.8f' % (float(trade['qty']) * price))
                if buy_order['status'] == 'NEW':
                    continue

                # filled or partially_filled
                if field_amount == 0:
                    fills = buy_order['fills']
                    for f in fills:
                        price = float('%.2f' % float(f['price']))
                        qty = float('%.8f' % float(f['qty']))
                        fee = float('%.8f' % float(f['commission']))
                        field_amount += qty
                        field_cash_amount += price * qty
                        field_fees += fee

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))

                self.binance_trade_btc += field_amount
                self.binance_trade_usdt -= field_cash_amount

                self.binance_usdt_dec = field_cash_amount
                self.binance_fees += field_fees

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

                sell_order = self.huobiSpot.send_order(btc_amount, 'api', 'btcusdt', 'sell-market')
                if sell_order['status'] != 'ok':
                    if sell_order['status'] == 'fail':
                        self.logger.error('sell failed : %s' % sell_order['msg'])
                    else:
                        self.logger.error('sell failed : %s' % sell_order['err-msg'])

                    self.logger.info('开始回滚')
                    self.rollback_binance_order(buy_order_id)
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
                        field_fees = sell_order_info['data']['field-fees']
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
                    self.btc_exchange_max = 0.065

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    float(field_amount), float(field_cash_amount)))

                self.huobi_trade_btc -= float(field_amount)
                self.huobi_trade_usdt += float(field_cash_amount)
                # 更新交易量统计
                self.usdt_exchange_amount += float(field_cash_amount)
                self.btc_exchange_amount += float(field_amount)

                self.huobi_usdt_inc = float(field_cash_amount)
                self.huobi_fees += float(field_fees)

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
                self.logger.info('总手续费：%s USDT\t%s BNB' % (self.huobi_fees, self.binance_fees))
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
            elif float(b_bids[0][0]) / h_asks[0][
                0] > 1 + self.huobi_fee_rate + self.binance_fee_rate + self.binance_profit_rate:
                self.logger.info('binance 卖出, huobi买入')

                b = [(float(i[0]), float(i[1])) for i in b_bids]
                print b
                b_sum = np.sum(b[:3], axis=0)
                b_std = np.std(b[:3], axis=0)
                b_avg = np.mean(b[:3], axis=0)

                print h_asks
                h_sum = np.sum(h_asks[:3], axis=0)
                h_std = np.std(h_asks[:3], axis=0)
                h_avg = np.mean(h_asks[:3], axis=0)

                self.logger.info('ASKS:\tsum:%10.4f\tstd:%10.4f\tavg:%10.4f' % (b_sum[1], b_std[0], b_avg[0]))
                self.logger.info('BIDS:\tsum:%10.4f\tstd:%10.4f\tavg:%10.4f' % (h_sum[1], h_std[0], h_avg[0]))

                self.logger.info(
                    '卖出价:%s, 买入价:%s, 比例:%s' % (float(b_bids[0][0]), h_asks[0][0], float(b_bids[0][0]) / h_asks[0][0]))
                if h_std[0] > self.STD_THD or h_sum[1] < self.SUM_THD:
                    self.logger.info('标准差过大，本单取消')
                    time.sleep(0.1)
                    continue
                order_price = b_bids[0][0] - 0.1

                btc_amount = float('%.4f' % min(float(b_bids[0][1]), h_asks[0][1],
                                                self.btc_exchange_max))
                if btc_amount > float(b_bids[0][1]):
                    btc_amount = float(re.match('(\d+\.\d{4})\d*', '{:.8f}'.format(float(b_bids[0][1]))).group(1))

                if btc_amount > self.binance_trade_btc:
                    btc_amount = float(
                        re.match('(\d+\.\d{4})\d*', '{:.8f}'.format(self.binance_trade_btc)).group(1))

                usdt_amount = float('%.4f' % (btc_amount * h_asks[0][0]))

                self.logger.info('本次交易量：%s BTC, %s USDT' % (btc_amount, usdt_amount))
                if btc_amount < self.btc_exchange_min:
                    self.logger.info('BTC交易数量不足: %s, 本单取消' % self.btc_exchange_min)
                    time.sleep(1)
                    continue
                if float('%.4f' % (btc_amount * order_price)) < self.usdt_exchange_min:
                    self.logger.info('USDT交易数量不足: %s, 本单取消' % self.usdt_exchange_min)
                    time.sleep(1)
                    continue
                if usdt_amount > self.huobi_trade_usdt - 5:
                    self.logger.info('Huobi USDT 数量: %s, 不足：%s, 本单取消' % (self.huobi_trade_usdt, usdt_amount))
                    time.sleep(1)
                    continue

                # 限价卖
                self.logger.info('开始限价卖出')
                try:
                    sell_r = self.binanceClient.order_limit_sell(symbol='BTCUSDT', quantity=btc_amount,
                                                                 price=order_price, newOrderRespType='FULL')
                except Exception as e:
                    self.logger.error(u'Binance卖出错误: %s' % e)
                    time.sleep(3)
                    continue
                print sell_r
                sell_order_id = sell_r['orderId']
                self.output.write('\n' + str(sell_order_id))
                self.output.flush()
                self.logger.info('binance sell orderId: %s, state: %s' % (sell_order_id, sell_r['status']))
                field_cash_amount = 0
                field_amount = 0
                field_fees = 0
                if sell_r['status'] == 'NEW' or sell_r['status'] == 'PARTIALLY_FILLED':
                    # 撤销未完成订单
                    self.logger.info('撤消未完成委托')
                    try:
                        cancel_r = self.binanceClient.cancel_order(symbol='BTCUSDT', orderId=sell_order_id)
                        print cancel_r
                    except Exception as e:
                        self.logger.error(u'撤销错误: %s' % e)
                        times = 0
                        while times < 10:
                            self.logger.info(u'第%s次查询Binance订单状态' % times)
                            try:
                                order = self.binanceClient.get_order(symbol='BTCUSDT', orderId=sell_order_id)
                                sell_r['status'] = order['status']
                                self.logger.info(u'当前订单状态为: %s', order['status'])
                                # 撤销失败，状态必定为FILLED?
                                if sell_r['status'] == 'FILLED':
                                    self.logger.info('计算本次成交量')
                                    try:
                                        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT')
                                    except Exception as e:
                                        self.logger.error('Binance get_my_trades error' % e)
                                        continue
                                    else:
                                        print binance_trades
                                        for trade in binance_trades:
                                            if trade['orderId'] == sell_order_id:
                                                field_fees += float('%.8f' % float(trade['commission']))
                                                field_amount += float('%.8f' % float(trade['qty']))
                                                price = float('%.2f' % float(trade['price']))
                                                field_cash_amount += float('%.8f' % (float(trade['qty']) * price))
                                        break
                            except Exception as e:
                                self.logger.error(u'get order or get trades error: %s' % e)
                            times += 1

                        if times == 10:
                            self.logger.info('未知错误,程序终止')
                            break
                    else:
                        self.logger.info('撤销成功')
                        self.logger.info('更新本次成交量')
                        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT')
                        print binance_trades
                        for trade in binance_trades:
                            if trade['orderId'] == sell_order_id:
                                field_fees += float('%.8f' % float(trade['commission']))
                                field_amount += float('%.8f' % float(trade['qty']))
                                price = float('%.2f' % float(trade['price']))
                                field_cash_amount += float('%.8f' % (float(trade['qty']) * price))
                if sell_r['status'] == 'NEW':
                    continue

                # filled or partially_filled
                if field_amount == 0:
                    fills = sell_r['fills']
                    for f in fills:
                        price = float('%.2f' % float(f['price']))
                        qty = float('%.8f' % float(f['qty']))
                        fee = float('%.8f' % float(f['commission']))
                        field_amount += qty
                        field_cash_amount += price * qty
                        field_fees += fee

                # 更新统计数据
                self.btc_exchange_amount += float(field_amount)
                self.usdt_exchange_amount += float(field_cash_amount)

                # update income
                self.binance_usdt_inc = field_cash_amount
                self.binance_fees += field_fees

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
                    self.logger.info('本次交易终止，开始回滚')
                    # self.rollback_binance_order(sell_order_id)
                    self.untreated_btc -= field_amount
                    time.sleep(3)
                    continue
                else:
                    self.untreated_btc -= field_amount - btc_amount

                buy_r = self.huobiSpot.send_order(btc_amount, 'api', 'btcusdt', 'buy-limit', buy_price)
                print buy_r

                if buy_r['status'] != 'ok':
                    if buy_r['status'] == 'fail':
                        self.logger.error('buy failed : %s' % buy_r['msg'])
                    else:
                        self.logger.error('buy failed : %s' % buy_r['err-msg'])
                    self.logger.info('开始回滚')
                    self.rollback_binance_order(sell_order_id)
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
                        field_fees = float('%.8f' % (field_cash_amount * 0.002))
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
                    self.btc_exchange_max = 0.065

                self.logger.info('field_amount:%.8f\tfield_cash_amount:%.8f' % (
                    field_amount, field_cash_amount))
                # update income
                self.huobi_usdt_dec = field_cash_amount
                self.huobi_fees += field_fees

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
                self.logger.info('总手续费：%s USDT\t%s BNB' % (self.huobi_fees, self.binance_fees))
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
            time.sleep(0.1)
            nowtime = time.strftime('%H:%M:%S', time.localtime(time.time()))
            if nowtime.startswith('08:30'):
                self.order_statistics()

                self.huobi_fees = 0
                self.binance_fees = 0
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
                self.update_account_info()
                total_btc_amount_after = self.binance_trade_btc + self.huobi_trade_btc
                if abs(total_btc_amount_after - total_btc_amount_before) > 0.001:
                    self.logger.info('账户BTC总量发生异常，程序终止')
                    break
                self.last_deal_time = 0

    def rollback_binance_order(self, orderId):
        order_info = self.binanceClient.get_order(symbol='BTCUSDT', orderId=orderId)
        side = order_info['side'].upper()
        field_amount = float('%.6f' % float(order_info['executedQty']))
        price = float('%.2f' % float(order_info['price']))
        if side == 'BUY':
            try:
                order = self.binanceClient.order_limit_sell(symbol='BTCUSDT', quantity=field_amount,
                                                            price=price - 5, newOrderRespType='FULL')
                print order
            except Exception as e:
                self.logger.error(u'Binance卖出错误: %s, 回滚失败' % e)
        else:
            try:
                order = self.binanceClient.order_limit_buy(symbol='BTCUSDT', quantity=field_amount,
                                                           price=price + 5, newOrderRespType='FULL')
                print order
            except Exception as e:
                self.logger.error(u'Binance买入错误: %s, 回滚失败' % e)

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
        binance_trades = self.binanceClient.get_my_trades(symbol='BTCUSDT', recvWindow=130000)
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

        # print huobi_order_list
        # print binance_order_list

        yestoday = datetime.date.today() + datetime.timedelta(days=-1)
        timearray = time.strptime(str(yestoday) + ' 8:30:00', "%Y-%m-%d %H:%M:%S")
        timestamp = int(round(time.mktime(timearray)) * 1000)
        print timestamp
        workbook = xlsxwriter.Workbook('output.xlsx')
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

        total_btc_earnings = 0
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
            header = [u'BTC成交量', u'USDT盈亏', u'HUOBI手续费', u'BINANCE手续费']
            i = 0
            for h in header:
                worksheet3.write(row_3, i, h)
                i += 1
            row_3 += 1
            worksheet3.write(row_3, 0, total_btc_exchange)
            worksheet3.write(row_3, 1, total_usdt_earnings)
            worksheet3.write(row_3, 2, total_huobi_commission)
            worksheet3.write(row_3, 3, total_binance_commission)
        workbook.close()
        # 发送邮件
        self.logger.info('邮件通知')
        ArbitrageStratety.send_mail_with_attachment()

    @staticmethod
    def send_mail_with_attachment():
        from email import encoders
        from email.header import Header
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        from email.utils import parseaddr, formataddr
        from email.mime.text import MIMEText
        import smtplib

        def _format_addr(s):
            name, addr = parseaddr(s)
            return formataddr(( \
                Header(name, 'utf-8').encode(), \
                addr.encode('utf-8') if isinstance(addr, unicode) else addr))

        from_addr = 'otf955613631@163.com'
        username = 'otf955613631'
        password = 'k387166'
        to_addr = '18118999630@189.cn'
        smtp_server = 'smtp.163.com'

        print 'sending mail to 18118999630@189.cn'
        msg = MIMEMultipart()
        msg['From'] = _format_addr(from_addr)
        msg['To'] = _format_addr(to_addr)
        msg['Subject'] = u'收益情况(%s)' % (time.strftime('%Y-%m-%d', time.localtime(time.time())))
        msg.attach(MIMEText('send with file...', 'plain', 'utf-8'))

        # add file:
        with open('output.xlsx', 'rb') as f:
            mime = MIMEBase('text', 'txt', filename='output.xlsx')
            mime.add_header('Content-Disposition', 'attachment', filename='output.xlsx')
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            mime.set_payload(f.read())
            encoders.encode_base64(mime)
            msg.attach(mime)

        server = smtplib.SMTP()
        server.connect(smtp_server)
        server.login(username, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()


if __name__ == '__main__':
    strategy = ArbitrageStratety()
    strategy.update_account_info()
    strategy.go()
    # strategy.order_statistics()
    # trategy.rollback_binance_order('72499288')
