# -*- coding: utf-8 -*-
from OKEXService import *
import logging
import sys
import time
import numpy as np


class TermArbitrage:
    OKEX_ACCESS_KEY = 'ee02b2e1-b183-410d-883f-c22afc81b12e'
    OKEX_SECRET_KEY = '983676E3B392EF4AA67F1F7CF06E2ABB'

    # 正表示期货价格高于现货
    open_positive_thd = 70
    close_positive_thd = 10

    open_negative_thd = -70
    close_negetive_thd = -10

    margin_coefficient = 0.5
    min_contract_amount = 1

    symbol = 'btc_usdt'
    contract_type = 'this_week'

    def __init__(self):
        self.future_client = OkexFutureClient(self.OKEX_ACCESS_KEY, self.OKEX_SECRET_KEY)
        self.spot_client = OkexSpotClient(self.OKEX_ACCESS_KEY, self.OKEX_SECRET_KEY)

        # 多头合约数量
        self.bear_amount = 0
        # 空头合约数量
        self.bull_amount = 0

        #
        self.buy_available = 0
        self.sell_available = 0
        self.buy_profit_lossratio = 0
        self.sell_profit_lossratio = 0
        self.sell_price_avg = 0
        self.buy_price_avg = 0

        self.future_balance = 0
        self.future_rights = 0
        self.future_index = 0

        self.spot_free_btc = 0
        self.spot_free_usdt = 0
        self.spot_freezed_btc = 0
        self.spot_freezed_usdt = 0

        # config logging
        self.logger = logging.getLogger("Future")

        # 指定logger输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

        # 文件日志
        file_handler = logging.FileHandler("term.log")
        file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式

        # 控制台日志
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.formatter = formatter  # 也可以直接给formatter赋值

        # 为logger添加的日志处理器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # 指定日志的最低输出级别，默认为WARN级别
        self.logger.setLevel(logging.INFO)

    def update_account_info(self):
        self.logger.info('获取Future账户信息')
        try:
            future_info = self.future_client.userinfo_4fix()
            print future_info
        except Exception as e:
            self.logger.error(u'获取future账户信息异常: %s' % e)
        else:
            if future_info['result']:
                btc_info = future_info['info']['btc']
                self.future_balance = btc_info['balance']
                self.future_rights = btc_info['rights']
                self.logger.info('balance: %s' % self.future_balance)
                self.logger.info('rights: %s' % self.future_rights)

        self.logger.info('逐仓用户持仓查询')
        try:
            info = self.future_client.position_4fix(self.symbol, self.contract_type)
            print info
            if info['result']:
                holding = info['holding']
                if len(holding) > 0:
                    # hold是数组,暂不清楚数量大于1的情况
                    self.bear_amount = holding['buy_amount']
                    self.bull_amount = holding['sell_amount']
                    self.buy_available = holding['buy_available']
                    self.sell_available = holding['sell_available']
                    self.buy_profit_lossratio = holding['buy_profit_lossratio']
                    self.sell_profit_lossratio = holding['sell_profit_lossratio']
                    self.buy_price_avg = holding['buy_price_avg']
                    self.sell_price_avg = holding['sell_price_avg']
                else:
                    self.logger.info('用户未持仓')
            else:
                self.logger.info('postion_4fix result error')
        except Exception as e:
            self.logger.error('逐仓用户持仓查询异常: %s' % e)

        self.logger.info('获取spot账户信息')
        try:
            spot_info = self.spot_client.balances()
        except Exception as e:
            self.logger.error(u'获取spot账户信息异常: %s' % e)
        else:
            print spot_info
            if spot_info['result']:
                self.spot_free_btc = spot_info['info']['funds']['free']['btc']
                self.spot_free_usdt = spot_info['info']['funds']['free']['usdt']
                self.spot_freezed_btc = spot_info['info']['funds']['freezed']['btc']
                self.spot_freezed_usdt = spot_info['info']['funds']['freezed']['usdt']
            print self.spot_free_btc, self.spot_free_usdt

    def calc_available_contract_amount(self):
        self.logger.info(u'获取OKEX指数信息')
        try:
            index_info = self.future_client.index(self.symbol)
        except Exception as e:
            self.logger.error(u'获取OKEX指数信息错误: %s' % e)
        else:
            self.future_index = index_info['future_index']
            self.logger.info('index: %s' % self.future_index)
        avialable_contract_amount = int(
            self.future_balance * self.future_index / 10 * self.margin_coefficient)

        return avialable_contract_amount

    def if_sufficient(self, type, arr):
        sum = np.sum(arr[:3], axis=0)
        std = np.std(arr[:3], axis=0)
        if type == 'spot':
            pass
        else:
            pass
        return True

    def go(self):
        while True:
            self.logger.info('获取期货深度')
            try:
                future_depth = self.future_client.depth(self.symbol, self.contract_type, 10)
            except Exception as e:
                self.logger.error(u'获取期货市场深度错误: %s' % e)
                time.sleep(3)
                continue
            future_bids = future_depth['bids']
            future_asks = future_depth['asks']

            print future_bids
            print future_asks
            self.logger.info('获取现货市场深度')
            try:
                spot_depth = self.spot_client.depth(self.symbol, 10)
            except Exception as e:
                self.logger.error(u'获取现货市场深度错误: %s' % e)
                time.sleep(3)
                continue
            spot_bids = spot_depth['bids']
            spot_asks = spot_depth['asks']

            print spot_bids
            print spot_asks
            # 开空
            if future_bids[0][0] - float(spot_asks[0][0]) > self.open_positive_thd:
                self.logger.info('期货开空，现货卖出')
                self.logger.info('计算可购合约数量')
                avialable_bear_amount = int(
                    self.future_balance * future_bids[0][0] / 10 * self.margin_coefficient)
                if avialable_bear_amount == 0:
                    self.logger.info('可购合约数量不足')
                    continue
            # 平空
            if future_bids[0][0] - float(spot_asks[0][0]) < self.close_positive_thd:
                if self.bear_amount == 0:
                    continue
                self.logger.info('期货平空，现货买入')
            # 开多
            if future_asks[0][0] - float(spot_bids[0][0]) < self.open_negative_thd:
                self.logger.info('期货开多，现货卖出')
                self.logger.info('计算可购合约数量')
                avialable_bull_amount = int(
                    self.future_balance * future_asks[0][0] / 10 * self.margin_coefficient)
                if avialable_bull_amount == 0:
                    self.logger.info('可购合约数量不足')
                    continue
                # 限价购买期货合约
                price = future_asks[0][0]
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  avialable_bull_amount, OPEN_LONG, MATCH_PRICE_FALSE)
                except Exception as e:
                    self.logger.error(u'Future订单异常: %s' % e)
                    continue
                print future_order
                if 'error_code' in future_order:
                    self.logger.error(u'Future订单错误: APIError(code=%s)' % future_order['error_code'])
                    break
                orderid = future_order['order_id']
                print orderid
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error(u'获取订单信息异常: %s' % e)
                    continue
                print order_info
                if 'error_code' in order_info:
                    self.logger.error(u'获取订单信息错误: APIError(code=%s)' % order_info['error_code'])
                    break
                self.logger.info('order status: %s' % order_info['stauts'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
                        # 撤销之后，不管有没有撤销成功，都要更新订单状态
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            if 'error_code' in order_info:
                                self.logger.error(u'查询订单信息错误: %s' % order_info['error_code'])
                                break
                            self.logger.info(u'当前订单状态: %s' % order_info['status'])
                        except Exception as e:
                            self.logger.error(u'查询订单信息异常: %s' % e)

                    except Exception as e:
                        self.logger.error(u'撤销订单异常: %s' % e)
                        break

                print order_info
                # 等待成交
                if order_info['status'] == 0:
                    continue

                future_deal_price = order_info['price_avg']
                future_deal_contract_amount = order_info['deal_amount']
                future_deal_fee = order_info['fee']
                future_deal_btc_amount = future_deal_contract_amount / future_deal_price

                self.bear_amount += future_deal_contract_amount

                self.logger.info('deal_contract:%d\tdeal_btc_amount:%.8f' % (
                    future_deal_contract_amount, future_deal_btc_amount
                ))
                # 市价卖出现货, 限价模拟市价
                spot_limited_price = float(spot_bids[0][0]) - 20
                spot_btc_amount = float('%.4f' % (100 * future_deal_contract_amount / spot_limited_price))
                print spot_btc_amount

                try:
                    spot_order = self.spot_client.place_order(spot_btc_amount, spot_limited_price, 'sell', self.symbol)
                except Exception as e:
                    self.logger.error(u'spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break
                if 'error_code' in spot_order:
                    self.logger.info(u'spot result error: %s' % spot_order['error_code'])
                    self.logger.info('开始回滚')
                    # TODO
                    break
                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(self.symbol, orderid)
                    except Exception as e:
                        self.logger.error(u'查询订单信息异常: %s' % e)
                    print order_info
                    if 'error_code' in order_info:
                        self.logger.info(u'查询订单信息错误: %s' % order_info['error_code'])
                        continue
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot sell filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']
                        spot_amount = order_info['amount']

                        self.logger.info(
                            'spot_deal_amount: %s\tspot_avg_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('购买现货错误, 终止程序')
                    break

            # 平多
            if future_asks[0][0] - float(spot_bids[0][0]) > self.open_negative_thd:
                if self.bear_amount == 0:
                    continue
                self.logger.info('期货平多，现货买入')
                self.logger.info('当前持多仓: %s' % self.bear_amount)
                # 限价购买期货合约
                price = future_asks[0][0]
                try:
                    future_order = self.future_client.place_order(self.symbol, self.contract_type, price,
                                                                  self.bear_amount, 3, 0)
                except Exception as e:
                    self.logger.error(u'Future订单异常: %s' % e)
                    continue
                print future_order
                if 'error_code' in future_order:
                    self.logger.error(u'Future订单错误: APIError(code=%s)' % future_order['error_code'])
                    break
                orderid = future_order['order_id']
                try:
                    order_info = self.future_client.order_info(self.symbol, self.contract_type, 0,
                                                               orderid, 1, 5)
                except Exception as e:
                    self.logger.error(u'查询订单信息异常:%s' % e)
                    continue

                if 'error_code' in order_info:
                    self.logger.error(u'查询订单信息错误: APIError(code=%s)' % order_info['error_code'])
                    break
                self.logger.info('order status: %s' % order_info['stauts'])
                # 等待成交或未成交
                if order_info['status'] == 0 or order_info['status'] == 1:
                    self.logger.info('撤销未完成委托')
                    try:
                        self.future_client.cancel(self.symbol, self.contract_type, orderid)
                        # 撤销之后，不管有没有撤销成功，都要更新订单状态
                        try:
                            order_info = self.future_client.order_info(self.symbol, self.contract_type,
                                                                       0, orderid, 1, 5)
                            if 'error_code' in order_info:
                                self.logger.error(u'查询订单信息错误: %s' % order_info['error_code'])
                                break
                            self.logger.info(u'当前订单状态: %s' % order_info['status'])
                        except Exception as e:
                            self.logger.error(u'查询订单信息异常: %s' % e)

                    except Exception as e:
                        self.logger.error(u'撤销订单异常: %s' % e)
                        break

                print order_info
                # 等待成交
                if order_info['status'] == 0:
                    continue

                future_deal_price = order_info['price_avg']
                future_deal_contract_amount = order_info['deal_amount']
                future_deal_btc_amount = future_deal_contract_amount / future_deal_price

                self.bear_amount -= future_deal_contract_amount

                self.logger.info('deal_contract:%d\tdeal_btc_amount:%.8f' % (
                    future_deal_contract_amount, future_deal_btc_amount
                ))

                try:
                    spot_order = self.spot_client.place_order('', 100 * future_deal_contract_amount, 'buy_market',
                                                              self.symbol)
                except Exception as e:
                    self.logger.error(u'spot订单异常: %s' % e)
                    self.logger.info('开始回滚')
                    # TODO
                    break
                if 'error_code' in spot_order:
                    self.logger.info(u'spot result error: %s' % spot_order['error_code'])
                    self.logger.info('开始回滚')
                    # TODO
                    break
                print spot_order
                orderid = spot_order['order_id']
                times = 0
                while times < 20:
                    self.logger.info('第%s次确认订单信息' % (times + 1))
                    try:
                        order_info = self.spot_client.status_order(self.symbol, orderid)
                    except Exception as e:
                        self.logger.error(u'查询订单信息异常: %s' % e)
                    print order_info
                    if 'error_code' in order_info:
                        self.logger.info(u'查询订单信息错误: %s' % order_info['error_code'])
                        continue
                    # status:-1:已撤销  0:未成交  1:部分成交  2:完全成交 4:撤单处理中
                    if order_info['status'] == 2:
                        self.logger.info('spot sell filled')
                        spot_avg_price = order_info['avg_price']
                        spot_deal_amount = order_info['deal_amount']
                        spot_amount = order_info['amount']

                        self.logger.info(
                            'spot_deal_amount: %s\tspot_avg_price: %s' % (spot_deal_amount, spot_avg_price))
                        break

                    times += 1

                    if times == 19:
                        time.sleep(10)
                if times == 20:
                    self.logger.info('购买现货错误, 终止程序')
                    break
                break
            time.sleep(10)


if __name__ == '__main__':
    term = TermArbitrage()
    term.update_account_info()
    term.go()
    # term.update_position_4fix()
